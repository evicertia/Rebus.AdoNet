using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using Rebus.Logging;
using Rebus.Timeout;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a timeout storage for Rebus that stores sagas in AdoNet.
	/// </summary>
	public class AdoNetTimeoutStorage : IStoreTimeouts, AdoNetTimeoutStorageFluentConfigurer
	{
		static ILog log;

		readonly AdoNetConnectionFactory factory;
		readonly string timeoutsTableName;

		static AdoNetTimeoutStorage()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the timeout storage which will use the specified connection string to connect to a database,
		/// storing the timeouts in the table with the specified name
		/// </summary>
		public AdoNetTimeoutStorage(AdoNetConnectionFactory factory, string timeoutsTableName)
		{
			this.factory = factory;
			this.timeoutsTableName = timeoutsTableName;
		}

		/// <summary>
		/// Gets the name of the table where timeouts are stored
		/// </summary>
		public string TimeoutsTableName
		{
			get { return timeoutsTableName; }
		}

		/// <summary>
		/// Adds the given timeout to the table specified by <see cref="TimeoutsTableName"/>
		/// </summary>
		public void Add(Timeout.Timeout newTimeout)
		{
			using (var connection = factory.OpenConnection())
			using (var command = connection.CreateCommand())
			{
				var parameters = new Dictionary<string, object>
					{
						{ "time_to_return", newTimeout.TimeToReturn },
						{ "correlation_id", newTimeout.CorrelationId },
						{ "saga_id", newTimeout.SagaId },
						{ "reply_to", newTimeout.ReplyTo }
					};

				if (newTimeout.CustomData != null)
				{
					parameters.Add("custom_data", newTimeout.CustomData);
				}

				foreach (var parameter in parameters)
				{
					command.AddParameter(parameter.Key, parameter.Value);
				}

				const string sql = @"INSERT INTO ""{0}"" ({1}) VALUES ({2})";

				var valueNames = string.Join(", ", parameters.Keys.Select(x => "\"" + x + "\""));
				var parameterNames = string.Join(", ", parameters.Keys.Select(x => "@" + x));

				command.CommandText = string.Format(sql, timeoutsTableName, valueNames, parameterNames);

				command.ExecuteNonQuery();
			}
		}

		public string GetSqlForDialect(IDbConnection connection)
		{
			// PostgresSql Dialects
			if (factory.Dialect.SupportsPgTryAdvisoryXactLock || factory.Dialect.SupportsPgTryAdvisoryLock || factory.Dialect.SupportsSkipLocked)
			{
				return GetSqlForPostgreSqlDialect(new Version(factory.Dialect.GetDatabaseVersion(connection)));
			}

			string sql = @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
						FROM ""{0}""
						WHERE ""time_to_return"" <= @current_time
						ORDER BY ""time_to_return"" ASC";
			return sql;

		}

		public string GetSqlForPostgreSqlDialect(Version version)
		{
			// Version between 9.1 and 9.5
			if (version < new Version("9.5") && version > new Version("9.1"))
			{
				return @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
						FROM ""{0}""
						WHERE ""time_to_return"" <= @current_time AND pg_try_advisory_xact_lock(""id"")
						ORDER BY ""time_to_return"" ASC";
			}

			// Version leater than 9.5 or 9.5
			if (version == new Version("9.5") || version > new Version("9.5"))
			{
				return @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
						FROM ""{0}""
						WHERE ""time_to_return"" <= @current_time
						ORDER BY ""time_to_return"" ASC
						FOR UPDATE SKIP LOCKED";
			}

			// Version earlier than 9.1
			return @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
					FROM ""{0}""
					WHERE ""time_to_return"" <= @current_time AND pg_try_advisory_lock(""id"")
					ORDER BY ""time_to_return"" ASC";
		}

		/// <summary>
		/// Queries the underlying table and returns due timeouts, removing them at the same time
		/// </summary>
		public DueTimeoutsResult GetDueTimeouts()
		{
			var dueTimeouts = new List<DueTimeout>();

			var connection = factory.OpenConnection();
			var transaction = connection.BeginTransaction();

			using (var command = connection.CreateCommand())
			{
				command.Transaction = transaction;

				string sql = GetSqlForDialect(connection);

				command.CommandText = string.Format(sql, timeoutsTableName);

				command.AddParameter("current_time", RebusTimeMachine.Now());

				using (var reader = command.ExecuteReader())
				{
					while (reader.Read())
					{
						var sqlTimeout = DueAdoNetTimeout.Create(MarkAsProcessed, timeoutsTableName, reader, connection, transaction);

						dueTimeouts.Add(sqlTimeout);
					}
				}
			}

			return new DueTimeoutsResult(dueTimeouts, () => CommitAndClose(connection, transaction));
		}

		void CommitAndClose(IDbConnection connection, IDbTransaction transaction)
		{
			try
			{
				transaction.Commit();
				transaction.Dispose();
				connection.Dispose();
			}
			catch (Exception ex)
			{
				log.Error("CloseAndCommit produced an exception: {0}", ex.Message);
			}
		}

		void MarkAsProcessed(DueAdoNetTimeout dueTimeout)
		{
			using (var command = dueTimeout.Connection.CreateCommand())
			{
				command.CommandText = string.Format(@"DELETE FROM ""{0}"" WHERE ""id"" = @id", timeoutsTableName);
				command.AddParameter("id", dueTimeout.Id);

				command.ExecuteNonQuery();
			}
		}

		/// <summary>
		/// Creates the necessary timeout storage table if it hasn't already been created. If a table already exists
		/// with a name that matches the desired table name, no action is performed (i.e. it is assumed that
		/// the table already exists).
		/// </summary>
		public AdoNetTimeoutStorageFluentConfigurer EnsureTableIsCreated()
		{
			using (var connection = factory.OpenConnection())
			{
				var tableNames = factory.Dialect.GetTableNames(connection);

				if (tableNames.Contains(timeoutsTableName, StringComparer.OrdinalIgnoreCase))
				{
					return this;
				}

				log.Info("Table '{0}' does not exist - it will be created now", timeoutsTableName);

				using (var command = connection.CreateCommand())
				{
					command.CommandText = factory.Dialect.FormatCreateTable(
						new AdoNetTable()
						{
							Name = timeoutsTableName,
							Columns = new[]
							{
								new AdoNetColumn() { Name = "id", DbType = DbType.Int64, Identity = true },
								new AdoNetColumn() { Name = "time_to_return", DbType = DbType.DateTimeOffset },
								new AdoNetColumn() { Name = "correlation_id", DbType = DbType.String, Length = 200 },
								new AdoNetColumn() { Name = "saga_id", DbType = DbType.Guid },
								new AdoNetColumn() { Name = "reply_to", DbType = DbType.String, Length = 200 },
								new AdoNetColumn() { Name = "custom_data", DbType = DbType.String, Length = 1073741823 }
							},
							PrimaryKey = new[] { "id" },
							Indexes = new[]
							{
								new AdoNetIndex() { Name = "ix_" + timeoutsTableName + "_alarm", Columns = new[] { "time_to_return" } }
							}
						}
					);

					command.ExecuteNonQuery();
				}

			}

			return this;
		}

		public class DueAdoNetTimeout : DueTimeout
		{
			readonly Action<DueAdoNetTimeout> markAsProcessedAction;
			readonly string timeoutsTableName;
			readonly IDbConnection _connection;
			readonly IDbTransaction _transaction;

			public IDbConnection Connection => _connection;
			public IDbTransaction Transaction => _transaction;

			readonly long id;

			public DueAdoNetTimeout(Action<DueAdoNetTimeout> markAsProcessedAction, string timeoutsTableName, long id, string replyTo, string correlationId, DateTime timeToReturn, Guid sagaId, string customData, IDbConnection connection, IDbTransaction transaction)
				: base(replyTo, correlationId, timeToReturn, sagaId, customData)
			{
				this.markAsProcessedAction = markAsProcessedAction;
				this.timeoutsTableName = timeoutsTableName;
				this.id = id;
				this._connection = connection;
				this._transaction = transaction;
			}

			public static DueAdoNetTimeout Create(Action<DueAdoNetTimeout> markAsProcessedAction, string timeoutsTableName, IDataReader reader, IDbConnection connection, IDbTransaction transaction)
			{
				var id = (long)reader["id"];
				var correlationId = (string)reader["correlation_id"];
				var sagaId = (Guid)reader["saga_id"];
				var replyTo = (string)reader["reply_to"];
				var timeToReturn = (DateTime)reader["time_to_return"];
				var customData = (string)(reader["custom_data"] != DBNull.Value ? reader["custom_data"] : "");

				var timeout = new DueAdoNetTimeout(markAsProcessedAction, timeoutsTableName, id, replyTo, correlationId, timeToReturn, sagaId, customData, connection, transaction);

				return timeout;
			}

			public override void MarkAsProcessed()
			{
				markAsProcessedAction(this);
			}

			public long Id
			{
				get { return id; }
			}
		}
	}
}
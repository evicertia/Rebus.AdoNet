using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using Rebus.Logging;
using Rebus.Timeout;
using Rebus.AdoNet.Schema;
using Rebus.AdoNet.Dialects;

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
		readonly SqlDialect dialect;
		readonly uint batchSize;

		static AdoNetTimeoutStorage()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the timeout storage which will use the specified connection string to connect to a database,
		/// storing the timeouts in the table with the specified name
		/// </summary>
		public AdoNetTimeoutStorage(AdoNetConnectionFactory factory, string timeoutsTableName) : this(factory, timeoutsTableName, 0)
		{
		}

		/// <summary>
		/// Constructs the timeout storage which will use the specified connection string to connect to a database,
		/// storing the timeouts in the table with the specified name and limit
		/// </summary>
		public AdoNetTimeoutStorage(AdoNetConnectionFactory factory, string timeoutsTableName, uint batchSize)
		{
			this.factory = factory;
			this.timeoutsTableName = timeoutsTableName;
			this.dialect = factory.Dialect;
			this.batchSize = batchSize;
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

		private void SafeDispose(IDisposable obj, string message, params object[] args)
		{
			try
			{
				obj?.Dispose();
			}
			catch (Exception ex)
			{
				log.Error(ex, message, args);
			}
		}

		private void CommitAndClose(IDbConnection connection, IDbTransaction transaction)
		{
			try
			{
				transaction.Commit();
			}
			catch (Exception ex)
			{
				log.Error("Commiting the transaction produced an exception: {0}", ex);
			}
			finally
			{
				SafeDispose(transaction, "Disposing the transaction produced an exception.");
				SafeDispose(connection, "Disposing the connection produced an exception");
			}
		}

		#region GetDueTimeouts
		private static string FormatGetTimeoutsDueQuery(SqlDialect dialect, string tableName, uint batchSize)
		{

			var forUpdateStmt = string.Empty;
			var skipLockedStmt = string.Empty;
			var lockPredicate = string.Empty;
			var limitPredicate = string.Empty;

			if (dialect.SupportsSelectForUpdate && dialect.SupportsSelectForWithSkipLocked)
			{
				forUpdateStmt = dialect.SelectForUpdateClause;
				skipLockedStmt = dialect.SelectForSkipLockedClause;
			}
			else if (dialect.SupportsTryAdvisoryXactLockFunction)
			{
				lockPredicate = $"AND " + dialect.FormatTryAdvisoryXactLock(new[] { "id" });
			}
			else if (dialect.SupportsTryAdvisoryLockFunction)
			{
				lockPredicate = $"AND " + dialect.FormatTryAdvisoryLock(new[] { "id" });
			}

			if(batchSize != 0)
			{
				limitPredicate = "LIMIT " + batchSize;
			}

			return  $"SELECT id, time_to_return, correlation_id, saga_id, reply_to, custom_data " +
					$"FROM {tableName} " +
					$"WHERE time_to_return <= @current_time {lockPredicate} " +
					$"ORDER BY time_to_return ASC " +
					$"{forUpdateStmt} {skipLockedStmt} " +
					$"{limitPredicate}";
		}

		/// <summary>
		/// Queries the underlying table and returns due timeouts, removing them at the same time
		/// </summary>
		public DueTimeoutsResult GetDueTimeouts()
		{
			var dueTimeouts = new List<DueTimeout>();
			IDbConnection connection = null;
			IDbTransaction transaction = null;

			try
			{
				connection = factory.OpenConnection();
				transaction = connection.BeginTransaction();

				using (var command = connection.CreateCommand())
				{
					command.Transaction = transaction;
					command.CommandText = FormatGetTimeoutsDueQuery(dialect, timeoutsTableName, batchSize);
					command.AddParameter("current_time", RebusTimeMachine.Now());

					using (var reader = command.ExecuteReader())
					{
						while (reader.Read())
						{
							var sqlTimeout = DueAdoNetTimeout.Create(connection, timeoutsTableName, reader);

							dueTimeouts.Add(sqlTimeout);
						}
					}
				}
			}
			catch (Exception ex)
			{
				log.Error("GetDueTimeout produced an exception: {0}", ex);

				SafeDispose(transaction, "Disposing the transaction after exception produced other exception.");
				SafeDispose(connection, "Disposing the connection after exception produced other exception");
			}

			return new DueTimeoutsResult(dueTimeouts, () => CommitAndClose(connection, transaction));
		}
		#endregion

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
			readonly string timeoutsTableName;
			readonly IDbConnection connection;

			readonly long id;

			public long Id
			{
				get { return id; }
			}

			public DueAdoNetTimeout(IDbConnection connection, string timeoutsTableName, long id, string replyTo, string correlationId, DateTime timeToReturn, Guid sagaId, string customData)
				: base(replyTo, correlationId, timeToReturn, sagaId, customData)
			{
				this.timeoutsTableName = timeoutsTableName;
				this.id = id;
				this.connection = connection;
			}

			public static DueAdoNetTimeout Create(IDbConnection connection, string timeoutsTableName, IDataReader reader)
			{
				var id = (long)reader["id"];
				var correlationId = (string)reader["correlation_id"];
				var sagaId = (Guid)reader["saga_id"];
				var replyTo = (string)reader["reply_to"];
				var timeToReturn = (DateTime)reader["time_to_return"];
				var customData = (string)(reader["custom_data"] != DBNull.Value ? reader["custom_data"] : "");

				var timeout = new DueAdoNetTimeout(connection, timeoutsTableName, id, replyTo, correlationId, timeToReturn, sagaId, customData);

				return timeout;
			}

			public override void MarkAsProcessed()
			{
				lock (connection)
				{
					using (var command = connection.CreateCommand())
					{
						command.CommandText = string.Format(@"DELETE FROM ""{0}"" WHERE ""id"" = @id", timeoutsTableName);
						command.AddParameter("id", this.id);

						command.ExecuteNonQuery();
					}
				}
			}
		}
	}
}
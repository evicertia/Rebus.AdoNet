using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using Newtonsoft.Json;

using Rebus.Logging;
using Rebus.Serialization;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// </summary>
	public class AdoNetSagaPersister : AdoNetStorage, IStoreSagaData
	{
		private static ILog log;

		private const int MaximumSagaDataTypeNameLength = 40;

		private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings { TypeNameHandling = TypeNameHandling.All };

		private string sagaIndexTableName;
		private string sagaTableName;

		private string idPropertyName;
		private bool indexNullProperties = true;

		static AdoNetSagaPersister()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetSagaPersister(string connectionString, string providerName, string sagaTableName, string sagaIndexTableName)
			: base(connectionString, providerName)
		{
			Initialize(sagaTableName, sagaIndexTableName);
		}

		/// <summary>
		/// Constructs the persister with the ability to use an externally provided <see cref="IDbConnection"/>, thus allowing it
		/// to easily enlist in any ongoing SQL transaction magic that might be going on. This means that the perister will assume
		/// that someone else manages the connection's lifetime.
		/// </summary>
		public AdoNetSagaPersister(Func<ConnectionHolder> connectionFactoryMethod, string sagaTableName, string sagaIndexTableName)
			: base(connectionFactoryMethod)
		{
			Initialize(sagaTableName, sagaIndexTableName);
		}

		/// <summary>
		/// Returns the name of the table used to store correlation properties of saga instances
		/// </summary>
		public string SagaIndexTableName
		{
			get { return sagaIndexTableName; }
		}

		/// <summary>
		/// Returns the name of the table used to store JSON serializations of saga instances.
		/// </summary>
		public string SagaTableName
		{
			get { return sagaTableName; }
		}

		/// <summary>
		/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
		/// </summary>
		public AdoNetSagaPersister DoNotIndexNullProperties()
		{
			indexNullProperties = false;
			return this;
		}

		/// <summary>
		/// Creates the necessary saga storage tables if they haven't already been created. If a table already exists
		/// with a name that matches one of the desired table names, no action is performed (i.e. it is assumed that
		/// the tables already exist).
		/// </summary>
		public AdoNetSagaPersister EnsureTablesAreCreated()
		{
			var connection = getConnection();
			try
			{
				var tableNames = connection.GetTableNames();

				// bail out if there's already a table in the database with one of the names
				if (tableNames.Contains(SagaTableName, StringComparer.InvariantCultureIgnoreCase)
					|| tableNames.Contains(SagaIndexTableName, StringComparer.OrdinalIgnoreCase))
				{
					return this;
				}

				log.Info("Tables '{0}' and '{1}' do not exist - they will be created now",
						 SagaTableName, SagaIndexTableName);

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(@"
CREATE TABLE ""{0}"" (
	""id"" UUID NOT NULL,
	""revision"" INTEGER NOT NULL,
	""data"" TEXT NOT NULL,
	PRIMARY KEY (""id"")
);
", SagaTableName);
					command.ExecuteNonQuery();
				}

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(@"
CREATE TABLE ""{0}"" (
	""saga_type"" VARCHAR(40) NOT NULL,
	""key"" VARCHAR(200) NOT NULL,
	""value"" VARCHAR(200) NOT NULL,
	""saga_id"" UUID NOT NULL,
	PRIMARY KEY (""key"", ""value"", ""saga_type"")
);

CREATE INDEX ON ""{0}"" (""saga_id"");

", SagaIndexTableName);
					command.ExecuteNonQuery();
				}

				commitAction(connection);
			}
			finally
			{
				releaseConnection(connection);
			}
			return this;
		}

		public void Insert(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			var connection = getConnection();
			try
			{
				// next insert the saga
				using (var command = connection.CreateCommand())
				{
					command.AddParameter("id", sagaData.Id);

					sagaData.Revision++;
					command.AddParameter("revision", sagaData.Revision);
					command.AddParameter("data", JsonConvert.SerializeObject(sagaData, Formatting.Indented, Settings));

					command.CommandText = string.Format(@"insert into ""{0}"" (""id"", ""revision"", ""data"") values (@id, @revision, @data)", sagaTableName);

					try
					{
						command.ExecuteNonQuery();
					}
					catch (Exception exception)
					{
						if (AdoNetExceptionManager.IsOptimistickLockingException(exception))
							throw new OptimisticLockingException(sagaData, exception);
						
						throw;
					}
				}

				var propertiesToIndex = GetPropertiesToIndex(sagaData, sagaDataPropertyPathsToIndex);

				if (propertiesToIndex.Any())
				{
					CreateIndex(sagaData, connection, propertiesToIndex);
				}

				commitAction(connection);
			}
			finally
			{
				releaseConnection(connection);
			}
		}

		public void Update(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			var connection = getConnection();
			try
			{
				// first, delete existing index
				using (var command = connection.CreateCommand())
				{
					const string deleteSagaIndexSql = @"DELETE FROM ""{0}"" WHERE ""saga_id"" = @id;";

					command.CommandText = string.Format(deleteSagaIndexSql, sagaIndexTableName);
					command.AddParameter("id", sagaData.Id);
					command.ExecuteNonQuery();
				}

				// next, update or insert the saga
				using (var command = connection.CreateCommand())
				{
					command.AddParameter("id", sagaData.Id);
					command.AddParameter("current_revision", sagaData.Revision);

					sagaData.Revision++;
					command.AddParameter("next_revision", sagaData.Revision);
					command.AddParameter("data", JsonConvert.SerializeObject(sagaData, Formatting.Indented, Settings));

					const string updateSagaSql = @"UPDATE ""{0}"" SET ""data"" = @data, ""revision"" = @next_revision WHERE ""id"" = @id AND ""revision"" = @current_revision";

					command.CommandText = string.Format(updateSagaSql, sagaTableName);
					var rows = command.ExecuteNonQuery();
					if (rows == 0)
					{
						throw new OptimisticLockingException(sagaData);
					}
				}

				var propertiesToIndex = GetPropertiesToIndex(sagaData, sagaDataPropertyPathsToIndex);

				if (propertiesToIndex.Any())
				{
					CreateIndex(sagaData, connection, propertiesToIndex);
				}

				commitAction(connection);
			}
			finally
			{
				releaseConnection(connection);
			}
		}

		void CreateIndex(ISagaData sagaData, ConnectionHolder connection, IEnumerable<KeyValuePair<string, string>> propertiesToIndex)
		{
			var sagaTypeName = GetSagaTypeName(sagaData.GetType());
			var parameters = propertiesToIndex
				.Select((p, i) => new
				{
					PropertyName = p.Key,
					PropertyValue = p.Value ?? "",
					PropertyNameParameter = string.Format("@n{0}", i),
					PropertyValueParameter = string.Format("@v{0}", i)
				})
				.ToList();

			// lastly, generate new index
			using (var command = connection.CreateCommand())
			{
				// generate batch insert with SQL for each entry in the index
				var inserts = parameters
					.Select(a => string.Format(
						@"                      insert into ""{0}""
															(""saga_type"", ""key"", ""value"", ""saga_id"") 
														values 
															(@saga_type, {1}, {2}, @saga_id)",
						sagaIndexTableName, a.PropertyNameParameter, a.PropertyValueParameter));

				var sql = string.Join(";" + Environment.NewLine, inserts);

				command.CommandText = sql;

				foreach (var parameter in parameters)
				{
					command.AddParameter(parameter.PropertyNameParameter, DbType.String, parameter.PropertyName);
					command.AddParameter(parameter.PropertyValueParameter, DbType.String, parameter.PropertyValue);
				}

				command.AddParameter("saga_type", DbType.String, sagaTypeName);
				command.AddParameter("saga_id", DbType.Guid, sagaData.Id);

				command.ExecuteNonQuery();
			}
		}

		public void Delete(ISagaData sagaData)
		{
			var connection = getConnection();
			try
			{
				using (var command = connection.CreateCommand())
				{
					const string updateSagaSql = @"DELETE FROM ""{0}"" WHERE ""id"" = @id AND ""revision"" = @current_revision;";

					command.CommandText = string.Format(updateSagaSql, sagaTableName);
					command.AddParameter("id", sagaData.Id);
					command.AddParameter("current_revision", sagaData.Revision);

					var rows = command.ExecuteNonQuery();

					if (rows == 0)
					{
						throw new OptimisticLockingException(sagaData);
					}
				}

				using (var command = connection.CreateCommand())
				{
					const string deleteSagaIndexSql = @"DELETE FROM ""{0}"" WHERE ""saga_id"" = @id";

					command.CommandText = string.Format(deleteSagaIndexSql, sagaIndexTableName);
					command.AddParameter("id", sagaData.Id);
					command.ExecuteNonQuery();
				}

				commitAction(connection);
			}
			finally
			{
				releaseConnection(connection);
			}
		}

		public TSagaData Find<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage) where TSagaData : class, ISagaData
		{
			var connection = getConnection();
			try
			{
				using (var command = connection.CreateCommand())
				{
					if (sagaDataPropertyPath == idPropertyName)
					{
						var id = (fieldFromMessage is Guid) ? (Guid)fieldFromMessage : Guid.Parse(fieldFromMessage.ToString());
						const string sql = @"SELECT s.data FROM ""{0}"" s WHERE s.id = @value";

						command.CommandText = string.Format(sql, sagaTableName);
						command.AddParameter("value", id);
					}
					else
					{
						const string sql = @"
SELECT s.data 
FROM ""{0}"" s 
JOIN ""{1}"" i on s.id = i.saga_id 
WHERE i.""saga_type"" = @saga_type AND i.""key"" = @key AND i.value = @value
";

						command.CommandText = string.Format(sql, sagaTableName, sagaIndexTableName);

						command.AddParameter("key", sagaDataPropertyPath);
						command.AddParameter("saga_type", GetSagaTypeName(typeof(TSagaData)));
						command.AddParameter("value", (fieldFromMessage ?? "").ToString());
					}

					var value = (string)command.ExecuteScalar();

					if (value == null) return null;

					try
					{
						return (TSagaData)JsonConvert.DeserializeObject(value, Settings);
					}
					catch (Exception exception)
					{
						var message = string.Format("An error occurred while attempting to deserialize '{0}' into a {1}", value, typeof(TSagaData));

						throw new ApplicationException(message, exception);
					}
				}
			}
			finally
			{
				releaseConnection(connection);
			}
		}

		void Initialize(string sagaTableName, string sagaIndexTableName)
		{
			this.sagaTableName = sagaTableName;
			this.sagaIndexTableName = sagaIndexTableName;

			idPropertyName = Reflect<ISagaData>.GetPropertyName(x => x.Id);
		}

		List<KeyValuePair<string, string>> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
		{
			return sagaDataPropertyPathsToIndex
				.Select(path =>
				{
					var value = Reflect.Value(sagaData, path);
					return new KeyValuePair<string, string>(path, value != null ? value.ToString() : null);
				})
				.Where(kvp => indexNullProperties || kvp.Value != null)
				.ToList();
		}

		string GetSagaTypeName(Type sagaDataType)
		{
			var sagaTypeName = sagaDataType.Name;

			if (sagaTypeName.Length > MaximumSagaDataTypeNameLength)
			{
				throw new InvalidOperationException(
					string.Format(
						@"Sorry, but the maximum length of the name of a saga data class is currently limited to {0} characters!

This is due to a limitation in SQL Server, where compound indexes have a 900 byte upper size limit - and
since the saga index needs to be able to efficiently query by saga type, key, and value at the same time,
there's room for only 200 characters as the key, 200 characters as the value, and 40 characters as the
saga type name.",
						MaximumSagaDataTypeNameLength));
			}

			return sagaTypeName;
		}
	}
}
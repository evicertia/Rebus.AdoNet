using System;
using System.Data;
using System.Linq;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

using Rebus.Logging;
using Rebus.Serialization;
using Rebus.AdoNet.Schema;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// </summary>
	public class AdoNetSagaPersister : IStoreSagaData, AdoNetSagaPersisterFluentConfigurer, ICanUpdateMultipleSagaDatasAtomically
	{
		private const int MaximumSagaDataTypeNameLength = 80;
		private const string SAGA_ID_COLUMN = "id";
		private const string SAGA_TYPE_COLUMN = "saga_type";
		private const string SAGA_DATA_COLUMN = "data";
		private const string SAGA_REVISION_COLUMN = "revision";
		private const string SAGAINDEX_TYPE_COLUMN = "saga_type";
		private const string SAGAINDEX_KEY_COLUMN = "key";
		private const string SAGAINDEX_VALUE_COLUMN = "value";
		private const string SAGAINDEX_ID_COLUMN = "saga_id";
		private static ILog log;
		private static readonly JsonSerializerSettings Settings = new JsonSerializerSettings {
			TypeNameHandling = TypeNameHandling.All, // TODO: Make it configurable by adding a SagaTypeResolver feature.
			DateFormatHandling = DateFormatHandling.IsoDateFormat, // TODO: Make it configurable..
		};

		private readonly AdoNetUnitOfWorkManager manager;
		private readonly string sagaIndexTableName;
		private readonly string sagaTableName;
		private readonly string idPropertyName;
		private bool useSagaLocking;
		private bool useNoWaitSagaLocking;
		private bool indexNullProperties = true;
		private Func<Type, string> sagaNameCustomizer = null;

		static AdoNetSagaPersister()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetSagaPersister(AdoNetUnitOfWorkManager manager, string sagaTableName, string sagaIndexTableName)
		{
			this.manager = manager;
			this.sagaTableName = sagaTableName;
			this.sagaIndexTableName = sagaIndexTableName;
			this.idPropertyName = Reflect.Path<ISagaData>(x => x.Id);
		}

		/// <summary>
		/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
		/// </summary>
		public AdoNetSagaPersisterFluentConfigurer DoNotIndexNullProperties()
		{
			indexNullProperties = false;
			return this;
		}

		public AdoNetSagaPersisterFluentConfigurer UseLockingOnSagaUpdates(bool waitForLocks)
		{
			useSagaLocking = true;
			useNoWaitSagaLocking = !waitForLocks;
			return this;
		}

		/// <summary>
		/// Creates the necessary saga storage tables if they haven't already been created. If a table already exists
		/// with a name that matches one of the desired table names, no action is performed (i.e. it is assumed that
		/// the tables already exist).
		/// </summary>
		public AdoNetSagaPersisterFluentConfigurer EnsureTablesAreCreated()
		{
			using (var scope = manager.GetScope(autonomous: true))
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var tableNames = scope.GetTableNames();

				// bail out if there's already a table in the database with one of the names
				var sagaTableIsAlreadyCreated = tableNames.Contains(sagaTableName, StringComparer.InvariantCultureIgnoreCase);
				var sagaIndexTableIsAlreadyCreated = tableNames.Contains(sagaIndexTableName, StringComparer.OrdinalIgnoreCase);

				if (sagaTableIsAlreadyCreated && sagaIndexTableIsAlreadyCreated)
				{
					return this;
				}

				if (sagaTableIsAlreadyCreated || sagaIndexTableIsAlreadyCreated)
				{
					// if saga index is created, then saga table is not created and vice versa
					throw new ApplicationException(string.Format("Table '{0}' do not exist - you have to create it manually",
						sagaIndexTableIsAlreadyCreated ? sagaTableName : sagaIndexTableName));
				}

				log.Info("Tables '{0}' and '{1}' do not exist - they will be created now", sagaTableName, sagaIndexTableName);

				using (var command = connection.CreateCommand())
				{
					command.CommandText = scope.Dialect.FormatCreateTable(
						new AdoNetTable()
						{
							Name = sagaTableName,
							Columns = new []
							{
								new AdoNetColumn() { Name = SAGA_ID_COLUMN, DbType = DbType.Guid },
								new AdoNetColumn() { Name = SAGA_TYPE_COLUMN, DbType = DbType.StringFixedLength, Length = 80 },
								new AdoNetColumn() { Name = SAGA_REVISION_COLUMN, DbType = DbType.Int32 },
								new AdoNetColumn() { Name = SAGA_DATA_COLUMN, DbType = DbType.String, Length = 1073741823}
							},
							PrimaryKey = new[] { SAGA_ID_COLUMN }
						}
					);
					command.ExecuteNonQuery();
				}

				using (var command = connection.CreateCommand())
				{
					command.CommandText = scope.Dialect.FormatCreateTable(
						new AdoNetTable()
						{
							Name = sagaIndexTableName,
							Columns = new []
							{
								new AdoNetColumn() { Name = SAGAINDEX_TYPE_COLUMN, DbType = DbType.StringFixedLength, Length = 80 },
								new AdoNetColumn() { Name = SAGAINDEX_KEY_COLUMN, DbType = DbType.StringFixedLength, Length = 200  },
								new AdoNetColumn() { Name = SAGAINDEX_VALUE_COLUMN, DbType = DbType.StringFixedLength, Length = 200 },
								new AdoNetColumn() { Name = SAGAINDEX_ID_COLUMN, DbType = DbType.Guid }
							},
							PrimaryKey = new[] { SAGAINDEX_ID_COLUMN, SAGAINDEX_KEY_COLUMN  },
							Indexes = new []
							{
								new AdoNetIndex() { Name = "ix_sagaindexes_id", Columns = new[] { SAGAINDEX_ID_COLUMN } }
							}
						}
					);
					command.ExecuteNonQuery();
				}

				scope.Complete();
			}

			return this;
		}

		/// <summary>
		/// Customizes the saga names by invoking this customizer.
		/// </summary>
		/// <param name="customizer">The customizer.</param>
		/// <returns></returns>
		public AdoNetSagaPersisterFluentConfigurer CustomizeSagaNamesAs(Func<Type, string> customizer)
		{
			this.sagaNameCustomizer = customizer;
			return this;
		}

		public void Insert(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = manager.GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var tableNames = scope.GetTableNames();
				var sagaTypeName = GetSagaTypeName(sagaData.GetType());

				// next insert the saga
				using (var command = connection.CreateCommand())
				{
					command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
					command.AddParameter(dialect.EscapeParameter(SAGA_TYPE_COLUMN), sagaTypeName);
					command.AddParameter(dialect.EscapeParameter(SAGA_REVISION_COLUMN), ++sagaData.Revision);
					command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), JsonConvert.SerializeObject(sagaData, Formatting.Indented, Settings));

					command.CommandText = string.Format(
						@"insert into {0} ({1}, {2}, {3}, {4}) values ({5}, {6}, {7}, {8});",
						dialect.QuoteForTableName(sagaTableName),
						dialect.QuoteForColumnName(SAGA_ID_COLUMN),
						dialect.QuoteForColumnName(SAGA_TYPE_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN),
						dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
						dialect.EscapeParameter(SAGA_ID_COLUMN),
						dialect.EscapeParameter(SAGA_TYPE_COLUMN),
						dialect.EscapeParameter(SAGA_REVISION_COLUMN),
						dialect.EscapeParameter(SAGA_DATA_COLUMN)
					);

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception)
					{
						throw new OptimisticLockingException(sagaData, exception);
					}
				}

				var propertiesToIndex = GetPropertiesToIndex(sagaData, sagaDataPropertyPathsToIndex);

				if (propertiesToIndex.Any())
				{
					DeclareIndex(sagaData, scope, propertiesToIndex);
				}

				scope.Complete();
			}
		}

		public void Update(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = manager.GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var tableNames = scope.GetTableNames();

				// next, update or insert the saga
				using (var command = connection.CreateCommand())
				{
					command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
					command.AddParameter(dialect.EscapeParameter("current_revision"), sagaData.Revision);
					command.AddParameter(dialect.EscapeParameter("next_revision"), ++sagaData.Revision);
					command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), JsonConvert.SerializeObject(sagaData, Formatting.Indented, Settings));

					command.CommandText = string.Format(
						@"UPDATE {0} SET {1} = {2}, {3} = {4} " +
						@"WHERE {5} = {6} AND {7} = {8};",
						dialect.QuoteForTableName(sagaTableName),
						dialect.QuoteForColumnName(SAGA_DATA_COLUMN), dialect.EscapeParameter(SAGA_DATA_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("next_revision"),
						dialect.QuoteForColumnName(SAGA_ID_COLUMN), dialect.EscapeParameter(SAGA_ID_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("current_revision")
					);
					var rows = command.ExecuteNonQuery();
					if (rows == 0)
					{
						throw new OptimisticLockingException(sagaData);
					}
				}

				var propertiesToIndex = GetPropertiesToIndex(sagaData, sagaDataPropertyPathsToIndex);

				if (propertiesToIndex.Any())
				{
					DeclareIndex(sagaData, scope, propertiesToIndex);
				}

				scope.Complete();
			}
		}

		private void DeclareIndexUsingTableExpressions(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, string> propertiesToIndex)
		{
			var dialect = scope.Dialect;
			var connection = scope.Connection;

			var sagaTypeName = GetSagaTypeName(sagaData.GetType());
			var parameters = propertiesToIndex
				.Select((p, i) => new
				{
					PropertyName = p.Key,
					PropertyValue = p.Value ?? "",
					PropertyNameParameter = string.Format("n{0}", i),
					PropertyValueParameter = string.Format("v{0}", i)
				})
				.ToList();

			var values = parameters
				.Select(p => string.Format("({0}, {1}, {2}, {3})",
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
					dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN),
					dialect.EscapeParameter(p.PropertyNameParameter),
					dialect.EscapeParameter(p.PropertyValueParameter)
				));

			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"WITH existing AS (" +
						"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {6} " +
						"ON CONFLICT ({1}, {3}) DO UPDATE SET {4} = excluded.{4} " +
						"RETURNING {3}) " +
					"DELETE FROM {0} " +
					"WHERE {1} = {5} AND {3} NOT IN (SELECT {3} FROM existing);",
					dialect.QuoteForTableName(sagaIndexTableName),      //< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),    //< 1
					dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN),	//< 2
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 3
					dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),	//< 4
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),		//< 5
					string.Join(", ", values)							//< 6
				);

				foreach (var parameter in parameters)
				{
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, parameter.PropertyValue ?? "");
				}

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);

				try
				{
					command.ExecuteNonQuery();
				}
				catch (DbException exception)
				{
					throw new OptimisticLockingException(sagaData, exception);
				}
			}
		}

		private void DeclareIndexUsingReturningClause(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, string> propertiesToIndex)
		{
			var dialect = scope.Dialect;
			var connection = scope.Connection;
			var existingKeys = Enumerable.Empty<string>();

			var sagaTypeName = GetSagaTypeName(sagaData.GetType());
			var parameters = propertiesToIndex
				.Select((p, i) => new
				{
					PropertyName = p.Key,
					PropertyValue = p.Value ?? "",
					PropertyNameParameter = string.Format("n{0}", i),
					PropertyValueParameter = string.Format("v{0}", i)
				})
				.ToList();

			var values = parameters
				.Select(p => string.Format("({0}, {1}, {2}, {3})",
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
					dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN),
					dialect.EscapeParameter(p.PropertyNameParameter),
					dialect.EscapeParameter(p.PropertyValueParameter)
				));
						
			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5} " +
						"ON CONFLICT ({1}, {3}) DO UPDATE SET {4} = excluded.{4} " +
						"RETURNING {3};",
					dialect.QuoteForTableName(sagaIndexTableName),		//< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
					dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN),	//< 2
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 3
					dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),	//< 4
					string.Join(", ", values)							//< 5
				);

				foreach (var parameter in parameters)
				{
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, parameter.PropertyValue ?? "");
				}

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);

				try
				{
					using (var reader = command.ExecuteReader())
					{
						existingKeys = reader.AsEnumerable<string>(SAGAINDEX_KEY_COLUMN).ToArray();
					}
				}
				catch (DbException exception)
				{
					throw new OptimisticLockingException(sagaData, exception);
				}
			}

			var idx = 0;
			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"DELETE FROM {0} " +
					"WHERE {1} = {2} AND {3} NOT IN ({4});",
					dialect.QuoteForTableName(sagaIndexTableName),		//< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),		//< 2
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 3
					string.Join(", ", existingKeys.Select(k => dialect.EscapeParameter($"k{idx++}")))
				);

				for (int i = 0; i < existingKeys.Count(); i++)
				{
					command.AddParameter(dialect.EscapeParameter($"k{i}"), DbType.StringFixedLength, existingKeys.ElementAt(i).Trim());
				}

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);

				try
				{
					command.ExecuteNonQuery();
				}
				catch (DbException exception)
				{
					throw new OptimisticLockingException(sagaData, exception);
				}
			}
		}

		private void DeclareIndexUnoptimized(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, string> propertiesToIndex)
		{
			var connection = scope.Connection;
			var dialect = scope.Dialect;
			var sagaTypeName = GetSagaTypeName(sagaData.GetType());

			var existingKeys = Enumerable.Empty<string>();

			// Let's fetch existing keys..
			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"SELECT {1} FROM {0} WHERE {2} = {3};",
					dialect.QuoteForTableName(sagaIndexTableName),      //< 0
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),   //< 1
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),    //< 2
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN)		//< 3
				);

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

				try
				{
					using (var reader = command.ExecuteReader())
					{
						existingKeys = reader.AsEnumerable<string>(SAGAINDEX_KEY_COLUMN).ToArray();
					}
				}
				catch (DbException exception)
				{
					throw new OptimisticLockingException(sagaData, exception);
				}
			}

			// For each exisring key, update it's value..
			foreach (var key in existingKeys.Where(k => propertiesToIndex.Any(p => p.Key == k)))
			{
				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						"UPDATE {0} SET {1} = {2} " + 
						"WHERE {3} = {4} AND {5} = {6};",
						dialect.QuoteForTableName(sagaIndexTableName),		//< 0
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),	//< 1
						dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN),	//< 2
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 3
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),		//< 4
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 5
						dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN)		//< 6
					);

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), DbType.String, key);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), DbType.String, propertiesToIndex[key] ?? "");

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception)
					{
						throw new OptimisticLockingException(sagaData, exception);
					}
				}
			}

			var removedKeys = existingKeys.Where(x => !propertiesToIndex.ContainsKey(x)).ToArray();

			if (removedKeys.Length > 0)
			{
				// Remove no longer needed keys..
				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						"DELETE FROM {0} WHERE {1} = {2} AND {3} IN ({4});",
						dialect.QuoteForTableName(sagaIndexTableName),      //< 0
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),    //< 1
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),       //< 2
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),   //< 3
						string.Join(", ", existingKeys.Select((x, i) => dialect.EscapeParameter($"k{i}")))
					);

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

					for (int i = 0; i < existingKeys.Count(); i++)
					{
						command.AddParameter(dialect.EscapeParameter($"k{i}"), DbType.StringFixedLength, existingKeys.ElementAt(i).Trim());
					}

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception)
					{
						throw new OptimisticLockingException(sagaData, exception);
					}
				}
			}

			var parameters = propertiesToIndex
					.Where(x => !existingKeys.Contains(x.Key))
					.Select((p, i) => new
					{
						PropertyName = p.Key,
						PropertyValue = p.Value ?? "",
						PropertyNameParameter = string.Format("n{0}", i),
						PropertyValueParameter = string.Format("v{0}", i)
					})
					.ToList();

			if (parameters.Count > 0)
			{
				// Insert new keys..
				using (var command = connection.CreateCommand())
				{
					var values = parameters.Select(p => string.Format("({0}, {1}, {2}, {3})",
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
						dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN),
						dialect.EscapeParameter(p.PropertyNameParameter),
						dialect.EscapeParameter(p.PropertyValueParameter)
					));


					command.CommandText = string.Format(
						"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5};",
						dialect.QuoteForTableName(sagaIndexTableName),      //< 0
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
						dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN),	//< 2
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 3
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),	//< 4
						string.Join(", ", values)							//< 5
					);

					foreach (var parameter in parameters)
					{
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, parameter.PropertyValue ?? "");
					}

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception)
					{
						throw new OptimisticLockingException(sagaData, exception);
					}

				}
			}
		}

		private void DeclareIndex(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, string> propertiesToIndex)
		{
			var dialect = scope.Dialect;

			if (dialect.SupportsTableExpressions && dialect.SupportsReturningClause && dialect.SupportsOnConflictClause)
			{
				DeclareIndexUsingTableExpressions(sagaData, scope, propertiesToIndex);
			}
			else if (dialect.SupportsReturningClause && dialect.SupportsOnConflictClause)
			{
				DeclareIndexUsingReturningClause(sagaData, scope, propertiesToIndex);
			}
			else
			{
				DeclareIndexUnoptimized(sagaData, scope, propertiesToIndex);
			}
		}

		public void Delete(ISagaData sagaData)
		{
			using (var scope = manager.GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						@"DELETE FROM {0} WHERE {1} = {2} AND {3} = {4};",
						dialect.QuoteForTableName(sagaTableName),
						dialect.QuoteForColumnName(SAGA_ID_COLUMN), dialect.EscapeParameter(SAGA_ID_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("current_revision")
					);
					command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
					command.AddParameter(dialect.EscapeParameter("current_revision"), sagaData.Revision);

					var rows = command.ExecuteNonQuery();

					if (rows == 0)
					{
						throw new OptimisticLockingException(sagaData);
					}
				}

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						@"DELETE FROM {0} WHERE {1} = {2};",
						dialect.QuoteForTableName(sagaIndexTableName),
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN), dialect.EscapeParameter(SAGA_ID_COLUMN)
					);
					command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
					command.ExecuteNonQuery();
				}

				scope.Complete();
			}
		}

		private string GetSagaLockingClause(SqlDialect dialect)
		{
			if (useSagaLocking)
			{
				return useNoWaitSagaLocking
					? $"{dialect.SelectForUpdateClause} {dialect.SelectForNoWaitClause}"
					: dialect.SelectForUpdateClause;
			}

			return string.Empty;
		}

		public TSagaData Find<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage) where TSagaData : class, ISagaData
		{
			// FIXME: Skip filtering by saga-data, and instead let deserialization try to match returned data to TSagaData. (pruiz)

			using (var scope = manager.GetScope(autocomplete: true))
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var sagaType = GetSagaTypeName(typeof(TSagaData));

				if (useSagaLocking)
				{
					if (!dialect.SupportsSelectForUpdate)
						throw new InvalidOperationException($"You can't use saga locking for a Dialect {dialect.GetType()} that does not supports Select For Update.");

					if (useNoWaitSagaLocking && !dialect.SupportsSelectForWithNoWait)
						throw new InvalidOperationException($"You can't use saga locking with no-wait for a Dialect {dialect.GetType()} that does not supports no-wait clause.");
				}
					
				using (var command = connection.CreateCommand())
				{
					if (sagaDataPropertyPath == idPropertyName)
					{
						var id = (fieldFromMessage is Guid) ? (Guid)fieldFromMessage : Guid.Parse(fieldFromMessage.ToString());
						var idParam = dialect.EscapeParameter("id");
						var sagaTypeParam = dialect.EscapeParameter(SAGA_TYPE_COLUMN);

						command.CommandText = string.Format(
							@"SELECT s.{0} FROM {1} s WHERE s.{2} = {3} AND s.{4} = {5} {6}",
							dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
							dialect.QuoteForTableName(sagaTableName),
							dialect.QuoteForColumnName(SAGA_TYPE_COLUMN),
							sagaTypeParam,
							dialect.QuoteForColumnName(SAGA_ID_COLUMN),
							idParam,
							GetSagaLockingClause(dialect)
						);
						command.AddParameter(sagaTypeParam, sagaType);
						command.AddParameter(idParam, id);
					}
					else
					{
						command.CommandText = string.Format(
							@"SELECT s.{0} " +
							@"FROM {1} s " +
							@"JOIN {2} i on s.{3} = i.{4} " +
							@"WHERE s.{5} = {6} AND i.{7} = {8} AND i.{9} = {10} {11}",
							dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
							dialect.QuoteForTableName(sagaTableName),
							dialect.QuoteForTableName(sagaIndexTableName),
							dialect.QuoteForColumnName(SAGA_ID_COLUMN), dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),
							dialect.QuoteForColumnName(SAGA_TYPE_COLUMN), dialect.EscapeParameter(SAGA_TYPE_COLUMN),
							dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN), dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN),
							dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN),
							GetSagaLockingClause(dialect)
						);
						command.AddParameter(dialect.EscapeParameter(SAGA_TYPE_COLUMN), sagaType);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), sagaDataPropertyPath);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), (fieldFromMessage ?? "").ToString());
					}

					string value = null;

					try
					{
						value = (string)command.ExecuteScalar();
					}
					catch (DbException ex)
					{
						// When in no-wait saga-locking mode, inspect
						// exception and rethrow ex as SagaLockedException.
						if (useSagaLocking && useNoWaitSagaLocking)
						{
							if (dialect.IsSelectForNoWaitLockingException(ex))
								throw new AdoNetSagaLockedException(ex);
						}

						throw;
					}

					if (value == null) return null;

					try
					{
						return JsonConvert.DeserializeObject<TSagaData>(value, Settings);
					}
					catch { }

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
		}

		IDictionary<string, string> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
		{
			return sagaDataPropertyPathsToIndex
				.SelectMany(path =>
				{
					var value = Reflect.Value(sagaData, path);
					var result = new List<KeyValuePair<string, string>>();

					if ((value is IEnumerable) && !(value is string))
					{
						foreach (var item in (value as IEnumerable))
							result.Add(new KeyValuePair<string, string>(path, item?.ToString()));
					}
					else
					{
						result.Add(new KeyValuePair<string, string>(path, value?.ToString()));
					}

					return result;
				})
				.Where(kvp => indexNullProperties || kvp.Value != null)
				.ToDictionary(x => x.Key, x => x.Value);
		}

		#region Default saga name

		private static string GetClassName(Type type)
		{
			var classNameRegex = new Regex("^[a-zA-Z0-9_]*[\\w]", RegexOptions.IgnoreCase);
			var match = classNameRegex.Match(type.Name);

			if (!match.Success) throw new Exception($"Error trying extract name class from type: {type.Name}");

			return match.Value;
		}

		private static IEnumerable<string> GetGenericArguments(Type type)
		{
			return type.GetGenericArguments()
				.Select(x => x.Name)
				.ToList();
		}

		private static string GetDefaultSagaName(Type type)
		{
			var declaringType = type.DeclaringType;

			if (type.IsNested && declaringType != null)
			{
				if (declaringType.IsGenericType)
				{
					var className = GetClassName(declaringType);
					var genericArguments = GetGenericArguments(type).ToList();

					return genericArguments.Any()
						? $"{className}<{string.Join(",", genericArguments)}>"
						: $"{className}";
				}

				return declaringType.Name;
			}

			return type.Name;
		}

		#endregion

		private string GetSagaTypeName(Type sagaDataType)
		{
			var sagaTypeName = sagaNameCustomizer != null ? sagaNameCustomizer(sagaDataType) : GetDefaultSagaName(sagaDataType);

			if (sagaTypeName.Length > MaximumSagaDataTypeNameLength)
			{
				throw new InvalidOperationException(
					string.Format(
						@"Sorry, but the maximum length of the name of a saga data class is currently limited to {0} characters!

This is due to a limitation in SQL Server, where compound indexes have a 900 byte upper size limit - and
since the saga index needs to be able to efficiently query by saga type, key, and value at the same time,
there's room for only 200 characters as the key, 200 characters as the value, and 80 characters as the
saga type name.",
						MaximumSagaDataTypeNameLength));
			}

			return sagaTypeName;
		}
	}
}

﻿using System;
using System.Data;
using System.Linq;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

using Rebus.Logging;
using Rebus.Serialization;
using Rebus.AdoNet.Dialects;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// </summary>
	public class AdoNetSagaPersister : IStoreSagaData, AdoNetSagaPersisterFluentConfigurer, ICanUpdateMultipleSagaDatasAtomically
	{
		private const int MaximumSagaDataTypeNameLength = 40;
		private const string SAGA_ID_COLUMN = "id";
		private const string SAGA_TYPE_COLUMN = "saga_type";
		private const string SAGA_DATA_COLUMN = "data";
		private const string SAGA_REVISION_COLUMN = "revision";
		private const string SAGAINDEX_ID_COLUMN = "saga_id";
		private const string SAGAINDEX_KEY_COLUMN = "key";
		private const string SAGAINDEX_VALUE_COLUMN = "value";
		private const string SAGAINDEX_VALUES_COLUMN = "values";
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
		private bool useSqlArrays = false;
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

		#region AdoNetSagaPersisterFluentConfigurer

		/// <summary>
		/// Configures the persister to ignore null-valued correlation properties and not add them to the saga index.
		/// </summary>
		public AdoNetSagaPersisterFluentConfigurer DoNotIndexNullProperties()
		{
			indexNullProperties = false;
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
								new AdoNetColumn() { Name = SAGA_TYPE_COLUMN, DbType = DbType.String, Length = MaximumSagaDataTypeNameLength },
								new AdoNetColumn() { Name = SAGA_REVISION_COLUMN, DbType = DbType.Int32 },
								new AdoNetColumn() { Name = SAGA_DATA_COLUMN, DbType = DbType.String, Length = 1073741823 }
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
								new AdoNetColumn() { Name = SAGAINDEX_ID_COLUMN, DbType = DbType.Guid },
								new AdoNetColumn() { Name = SAGAINDEX_KEY_COLUMN, DbType = DbType.String, Length = 200  },
								new AdoNetColumn() { Name = SAGAINDEX_VALUE_COLUMN, DbType = DbType.String, Length = 200, Nullable = true },
								new AdoNetColumn() { Name = SAGAINDEX_VALUES_COLUMN, DbType = DbType.String, Length = 65535, Nullable = true, Array = dialect.SupportsArrayTypes }
							},
							PrimaryKey = new[] { SAGAINDEX_ID_COLUMN, SAGAINDEX_KEY_COLUMN },
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

		/// <summary>
		/// Enables locking of sagas as to avoid two or more workers to update them concurrently.
		/// </summary>
		/// <returns>The saga locking.</returns>
		public AdoNetSagaPersisterFluentConfigurer EnableSagaLocking()
		{
			useSagaLocking = true;
			return this;
		}

		/// <summary>
		/// Uses the use of sql array types for storing indexes related to correlation properties.
		/// </summary>
		/// <returns>The sql arrays.</returns>
		public AdoNetSagaPersisterFluentConfigurer UseSqlArraysForCorrelationIndexes()
		{
			useSqlArrays = true;
			return this;
		}

		#endregion

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

		private void DeclareIndexUsingTableExpressions(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, object> propertiesToIndex)
		{
			var dialect = scope.Dialect;
			var connection = scope.Connection;

			var sagaTypeName = GetSagaTypeName(sagaData.GetType());
			var parameters = propertiesToIndex
				.Select((p, i) => new
				{
					PropertyName = p.Key,
					PropertyValue = p.Value,
					PropertyNameParameter = string.Format("n{0}", i),
					PropertyValueParameter = string.Format("v{0}", i),
					PropertyValuesParameter = string.Format("vs{0}", i)
				})
				.ToList();

			var tuples = parameters
				.Select(p => string.Format("({0}, {1}, {2}, {3})",
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
					dialect.EscapeParameter(p.PropertyNameParameter),
					dialect.EscapeParameter(p.PropertyValueParameter),
					dialect.EscapeParameter(p.PropertyValuesParameter)
				));

			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"WITH existing AS (" +
						"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {6} " +
						"ON CONFLICT ({1}, {2}) DO UPDATE SET {3} = excluded.{3}, {4} = excluded.{4} " +
						"RETURNING {2}) " +
					"DELETE FROM {0} " +
					"WHERE {1} = {5} AND {2} NOT IN (SELECT {2} FROM existing);",
					dialect.QuoteForTableName(sagaIndexTableName),      //< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),    //< 1
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 2
					dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), //< 3
					dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 4
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),		//< 5
					string.Join(", ", tuples)							//< 6
				);

				foreach (var parameter in parameters)
				{
					var value = GetIndexValue(parameter.PropertyValue);

					command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, value);

					var values = ArraysEnabledFor(dialect)
						? (object)GetIndexValues(parameter.PropertyValue)?.ToArray()
						: GetConcatenatedIndexValues(GetIndexValues(parameter.PropertyValue));
					var dbtype = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValuesParameter), dbtype, values);
				}

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

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

		private void DeclareIndexUsingReturningClause(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, object> propertiesToIndex)
		{
			var dialect = scope.Dialect;
			var connection = scope.Connection;
			var existingKeys = Enumerable.Empty<string>();

			var sagaTypeName = GetSagaTypeName(sagaData.GetType());
			var parameters = propertiesToIndex
				.Select((p, i) => new
				{
					PropertyName = p.Key,
					PropertyValue = p.Value,
					PropertyNameParameter = string.Format("n{0}", i),
					PropertyValueParameter = string.Format("v{0}", i),
					PropertyValuesParameter = string.Format("vs{0}", i)
				})
				.ToList();

			var tuples = parameters
				.Select(p => string.Format("({0}, {1}, {2}, {3})",
					dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
					dialect.EscapeParameter(p.PropertyNameParameter),
					dialect.EscapeParameter(p.PropertyValueParameter),
					dialect.EscapeParameter(p.PropertyValuesParameter)
				));
						
			using (var command = connection.CreateCommand())
			{
				command.CommandText = string.Format(
					"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5} " +
						"ON CONFLICT ({1}, {2}) DO UPDATE SET {3} = excluded.{3}, {4} = excluded.{4} " +
						"RETURNING {2};",
					dialect.QuoteForTableName(sagaIndexTableName),		//< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 2
					dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), //< 3
					dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 4
					string.Join(", ", tuples)							//< 5
				);

				foreach (var parameter in parameters)
				{
					var value = GetIndexValue(parameter.PropertyValue);
					var values = GetConcatenatedIndexValues(GetIndexValues(parameter.PropertyValue));

					command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, value);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValuesParameter), DbType.String, values);
				}

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

		private void DeclareIndexUnoptimized(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, object> propertiesToIndex)
		{
			var connection = scope.Connection;
			var dialect = scope.Dialect;
			var sagaTypeName = GetSagaTypeName(sagaData.GetType());

			var idxTbl = dialect.QuoteForTableName(sagaIndexTableName);
			var idCol = dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN);
			var keyCol = dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN);
			var valueCol = dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN);
			var valuesCol = dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN);

			var idParam = dialect.EscapeParameter(SAGAINDEX_ID_COLUMN);

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
						"UPDATE {0} SET {1} = {2}, {3} = {4} " + 
						"WHERE {5} = {6} AND {7} = {8};",
						dialect.QuoteForTableName(sagaIndexTableName),		//< 0
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),	//< 1
						dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN),    //< 2
						dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 3
						dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN),   //< 4
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 5
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),		//< 6
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 7
						dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN)		//< 8
					);

					var value = GetIndexValue(propertiesToIndex[key]);
					var values = GetConcatenatedIndexValues(GetIndexValues(propertiesToIndex[key]));

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), DbType.String, key);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), DbType.String, value);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN), DbType.String, values);

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
						PropertyValue = p.Value,
						PropertyNameParameter = string.Format("n{0}", i),
						PropertyValueParameter = string.Format("v{0}", i),
						PropertyValuesParameter = string.Format("vs{0}", i)
					})
					.ToList();

			if (parameters.Count > 0)
			{
				// Insert new keys..
				using (var command = connection.CreateCommand())
				{

					var tuples = parameters.Select(p => string.Format("({0}, {1}, {2}, {3})",
						idParam,
						dialect.EscapeParameter(p.PropertyNameParameter),
						dialect.EscapeParameter(p.PropertyValueParameter),
						dialect.EscapeParameter(p.PropertyValuesParameter)
					));

					command.CommandText = string.Format(
						"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5};",
						dialect.QuoteForTableName(sagaIndexTableName),      //< 0
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 2
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), //< 3
						dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 4
						string.Join(", ", tuples)							//< 5
					);

					foreach (var parameter in parameters)
					{
						var value = GetIndexValue(parameter.PropertyValue);
						var values = GetConcatenatedIndexValues(GetIndexValues(parameter.PropertyValue));

						command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, value);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValuesParameter), DbType.String, values);
					}

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

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

		private void DeclareIndex(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IDictionary<string, object> propertiesToIndex)
		{
			var dialect = scope.Dialect;

			if (dialect.SupportsOnConflictClause && dialect.SupportsReturningClause && dialect.SupportsTableExpressions)
			{
				DeclareIndexUsingTableExpressions(sagaData, scope, propertiesToIndex);
			}
			else if (dialect.SupportsOnConflictClause && dialect.SupportsReturningClause)
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

		public TSagaData Find<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage) where TSagaData : class, ISagaData
		{
			using (var scope = manager.GetScope(autocomplete: true))
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var sagaType = GetSagaTypeName(typeof(TSagaData));

				if (useSagaLocking && !dialect.SupportsSelectForUpdate)
					throw new InvalidOperationException($"You can't use saga locking for a Dialect {dialect.GetType()} that is not supporting Select For Update");

				using (var command = connection.CreateCommand())
				{
					if (sagaDataPropertyPath == idPropertyName)
					{
						var id = (fieldFromMessage is Guid) ? (Guid)fieldFromMessage : Guid.Parse(fieldFromMessage.ToString());
						var idParam = dialect.EscapeParameter("id");
						var sagaTypeParam = dialect.EscapeParameter(SAGA_TYPE_COLUMN);

						command.CommandText = string.Format(
							@"SELECT s.{0} FROM {1} s WHERE s.{2} = {3} {4}",
							dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
							dialect.QuoteForTableName(sagaTableName),
							dialect.QuoteForColumnName(SAGA_ID_COLUMN),
							idParam,
							useSagaLocking ? dialect.ParameterSelectForUpdate : string.Empty
						);
						command.AddParameter(sagaTypeParam, sagaType);
						command.AddParameter(idParam, id);
					}
					else
					{
						var dataCol = dialect.QuoteForColumnName(SAGA_DATA_COLUMN);
						var sagaTblName = dialect.QuoteForTableName(sagaTableName);
						var indexTblName = dialect.QuoteForTableName(sagaIndexTableName);
						var sagaIdCol = dialect.QuoteForColumnName(SAGA_ID_COLUMN);
						var indexIdCol = dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN);
						var indexKeyCol = dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN);
						var indexKeyParam = dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN);
						var indexValueCol = dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN);
						var indexValueParm = dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN);
						var indexValuesCol = dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN);
						var indexValuesParm = dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN);
						var forUpdate = useSagaLocking ? dialect.ParameterSelectForUpdate : string.Empty;
						var valuesPredicate = ArraysEnabledFor(dialect)
							? $"(i.{indexValuesCol} @> {indexValuesParm})"
							: $"(i.{indexValuesCol} LIKE ('%' || {indexValuesParm} || '%'))";

						command.CommandText = $@"
							SELECT s.{dataCol}
							FROM {sagaTblName} s
							JOIN {indexTblName} i on s.{sagaIdCol} = i.{indexIdCol}
							WHERE i.{indexKeyCol} = {indexKeyParam}
							  AND (
							  		CASE WHEN {indexValueParm} IS NULL THEN i.{indexValueCol} IS NULL
									ELSE 
										(
											i.{indexValueCol} = {indexValueParm}
												OR
											(i.{indexValuesCol} is NOT NULL AND {valuesPredicate})
										)
									END
								  )
							{forUpdate};".Replace("\t", "");

						var value = GetIndexValue(fieldFromMessage);
						var values = value == null ? null : dialect.SupportsArrayTypes
							? (object)(new[] { value })
							: GetConcatenatedIndexValues(new[] { value });
						var dbtype = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), sagaDataPropertyPath);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), DbType.String, value);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN), dbtype, values);
					}

					var data = (string)command.ExecuteScalar();

					if (data == null) return null;

					try
					{
						return JsonConvert.DeserializeObject<TSagaData>(data, Settings);
					}
					catch { }

					try
					{
						return (TSagaData)JsonConvert.DeserializeObject(data, Settings);
					}
					catch (Exception exception)
					{
						var message = string.Format("An error occurred while attempting to deserialize '{0}' into a {1}", data, typeof(TSagaData));

						throw new ApplicationException(message, exception);
					}
				}
			}
		}

		private bool ArraysEnabledFor(SqlDialect dialect)
		{
			return useSqlArrays && dialect.SupportsArrayTypes;
		}

		private bool ShouldIndexValue(object value)
		{
			if (indexNullProperties)
				return true;

			if (value == null) return false;
			if (value is string) return true;
			if ((value is IEnumerable) && !(value as IEnumerable).Cast<object>().Any()) return false;

			return true;
		}

		private IDictionary<string, object> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
		{
			return sagaDataPropertyPathsToIndex
				.Select(x => new { Key = x, Value = Reflect.Value(sagaData, x) })
				.Where(ShouldIndexValue)
				.ToDictionary(x => x.Key, x => x.Value);
		}

		private static string GetIndexValue(object value)
		{
			if (value is string)
			{
				return value as string;
			}
			else if (value == null || value is IEnumerable)
			{
				return null;
			}

			return Convert.ToString(value);
		}

		private static IEnumerable<string> GetIndexValues(object value)
		{
			if (!(value is IEnumerable) || value is string)
			{
				return null;
			}

			return (value as IEnumerable).Cast<object>().Select(x => Convert.ToString(x)).ToArray();
		}

		private static string GetConcatenatedIndexValues(IEnumerable<string> values)
		{
			if (values == null || !values.Any())
			{
				return null;
			}

			var sb = new StringBuilder(values.Sum(x => x.Length + 1) + 1);
			sb.Append('|');

			foreach (var value in values)
			{
				sb.Append(value);
				sb.Append('|');
			}

			return sb.ToString();
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
there's room for only 200 characters as the key, 200 characters as the value, and 40 characters as the
saga type name.",
						MaximumSagaDataTypeNameLength));
			}

			return sagaTypeName;
		}
	}
}

using System;
using System.Data;
using System.Linq;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

using Rebus.Logging;
using Rebus.Serialization;
using Rebus.Serialization.Json;
using Rebus.AdoNet.Schema;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// </summary>
	public class AdoNetSagaPersisterLegacy : AdoNetSagaPersister, IStoreSagaData, AdoNetSagaPersisterFluentConfigurer, ICanUpdateMultipleSagaDatasAtomically
	{
		private const int MaximumSagaDataTypeNameLength = 80;
		private const string SAGA_ID_COLUMN = "id";
		private const string SAGA_TYPE_COLUMN = "saga_type";
		private const string SAGA_DATA_COLUMN = "data";
		private const string SAGA_REVISION_COLUMN = "revision";
		private const string SAGAINDEX_ID_COLUMN = "saga_id";
		private const string SAGAINDEX_KEY_COLUMN = "key";
		private const string SAGAINDEX_VALUE_COLUMN = "value";
		private const string SAGAINDEX_VALUES_COLUMN = "values";
		private static ILog log;

		private readonly string sagasIndexTableName;
		private readonly string sagasTableName;
		private readonly string idPropertyName;

		static AdoNetSagaPersisterLegacy()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetSagaPersisterLegacy(AdoNetUnitOfWorkManager manager, string sagasTableName, string sagasIndexTableName)
			: base(manager)
		{
			this.sagasTableName = sagasTableName;
			this.sagasIndexTableName = sagasIndexTableName;
			this.idPropertyName = Reflect.Path<ISagaData>(x => x.Id);
		}

		#region AdoNetSagaPersisterFluentConfigurer

		/// <summary>
		/// Creates the necessary saga storage tables if they haven't already been created. If a table already exists
		/// with a name that matches one of the desired table names, no action is performed (i.e. it is assumed that
		/// the tables already exist).
		/// </summary>
		public override AdoNetSagaPersisterFluentConfigurer EnsureTablesAreCreated()
		{
			using (var uow = Manager.Create(autonomous: true))
			using (var scope = (uow as IAdoNetUnitOfWork).GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var tableNames = scope.GetTableNames();

				// bail out if there's already a table in the database with one of the names
				var sagaTableIsAlreadyCreated = tableNames.Contains(sagasTableName, StringComparer.InvariantCultureIgnoreCase);
				var sagaIndexTableIsAlreadyCreated = tableNames.Contains(sagasIndexTableName, StringComparer.OrdinalIgnoreCase);

				if (sagaTableIsAlreadyCreated && sagaIndexTableIsAlreadyCreated)
				{
					log.Debug("Tables '{0}' and '{1}' already exists.", sagasTableName, sagasIndexTableName);
					return this;
				}

				if (sagaTableIsAlreadyCreated || sagaIndexTableIsAlreadyCreated)
				{
					// if saga index is created, then saga table is not created and vice versa
					throw new ApplicationException(string.Format("Table '{0}' do not exist - you have to create it manually",
						sagaIndexTableIsAlreadyCreated ? sagasTableName : sagasIndexTableName));
				}

				if (UseSqlArrays && !dialect.SupportsArrayTypes)
				{
					throw new ApplicationException("Enabled UseSqlArraysForCorrelationIndexes but underlaying database does not support arrays?!");
				}

				log.Info("Tables '{0}' and '{1}' do not exist - they will be created now", sagasTableName, sagasIndexTableName);

				using (var command = connection.CreateCommand())
				{
					command.CommandText = scope.Dialect.FormatCreateTable(
						new AdoNetTable()
						{
							Name = sagasTableName,
							Columns = new []
							{
								new AdoNetColumn() { Name = SAGA_ID_COLUMN, DbType = DbType.Guid },
								new AdoNetColumn() { Name = SAGA_TYPE_COLUMN, DbType = DbType.String, Length = MaximumSagaDataTypeNameLength },
								new AdoNetColumn() { Name = SAGA_REVISION_COLUMN, DbType = DbType.Int32 },
								new AdoNetColumn() { Name = SAGA_DATA_COLUMN, DbType = DbType.String, Length = 1073741823 }
							},
							PrimaryKey = new[] { SAGA_ID_COLUMN },
							Indexes = new []
							{
								new AdoNetIndex() { Name = $"ix_{sagasTableName}_{SAGA_ID_COLUMN}_{SAGA_TYPE_COLUMN}", Columns = new [] { SAGA_ID_COLUMN, SAGA_TYPE_COLUMN } },
							}
						}
					);

					command.ExecuteNonQuery();
				}

				using (var command = connection.CreateCommand())
				{
					command.CommandText = scope.Dialect.FormatCreateTable(
						new AdoNetTable()
						{
							Name = sagasIndexTableName,
							Columns = new []
							{
								new AdoNetColumn() { Name = SAGAINDEX_ID_COLUMN, DbType = DbType.Guid },
								new AdoNetColumn() { Name = SAGAINDEX_KEY_COLUMN, DbType = DbType.String, Length = 200  },
								new AdoNetColumn() { Name = SAGAINDEX_VALUE_COLUMN, DbType = DbType.String, Length = 1073741823, Nullable = true },
								new AdoNetColumn() { Name = SAGAINDEX_VALUES_COLUMN, DbType = DbType.String, Length = 1073741823, Nullable = true, Array = UseSqlArrays }
							},
							PrimaryKey = new[] { SAGAINDEX_ID_COLUMN, SAGAINDEX_KEY_COLUMN },
							Indexes = new []
							{
								new AdoNetIndex()
								{
									Name = $"ix_{sagasIndexTableName}_{SAGAINDEX_KEY_COLUMN}_{SAGAINDEX_VALUE_COLUMN}",
									Columns = new[] { SAGAINDEX_KEY_COLUMN, SAGAINDEX_VALUE_COLUMN },
								},
								new AdoNetIndex()
								{
									Name = $"ix_{sagasIndexTableName}_{SAGAINDEX_KEY_COLUMN}_{SAGAINDEX_VALUES_COLUMN}",
									Columns = new[] { SAGAINDEX_KEY_COLUMN, SAGAINDEX_VALUES_COLUMN },
								}
							}
						}
					);

					command.ExecuteNonQuery();
				}

				scope.Complete();
				log.Info("Tables '{0}' and '{1}' created", sagasTableName, sagasIndexTableName);
			}

			return this;
		}

		#endregion

		public override void Insert(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = Manager.GetScope())
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
					command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), Serialize(sagaData));

					command.CommandText = string.Format(
						@"insert into {0} ({1}, {2}, {3}, {4}) values ({5}, {6}, {7}, {8});",
						dialect.QuoteForTableName(sagasTableName),
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
					catch (DbException exception) when (
						dialect.IsOptimisticLockingException(exception)
						|| dialect.IsDuplicateKeyException(exception))
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

		public override void Update(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = Manager.GetScope())
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
					command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), Serialize(sagaData));

					command.CommandText = string.Format(
						@"UPDATE {0} SET {1} = {2}, {3} = {4} " +
						@"WHERE {5} = {6} AND {7} = {8};",
						dialect.QuoteForTableName(sagasTableName),
						dialect.QuoteForColumnName(SAGA_DATA_COLUMN), dialect.EscapeParameter(SAGA_DATA_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("next_revision"),
						dialect.QuoteForColumnName(SAGA_ID_COLUMN), dialect.EscapeParameter(SAGA_ID_COLUMN),
						dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("current_revision")
					);

					try
					{
						var rows = command.ExecuteNonQuery();
						if (rows == 0)
						{
							throw new OptimisticLockingException(sagaData);
						}
					}
					catch (DbException exception) when (
						dialect.IsOptimisticLockingException(exception)
						|| dialect.IsDuplicateKeyException(exception))
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
					dialect.QuoteForTableName(sagasIndexTableName),      //< 0
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
				catch (DbException exception) when (
					dialect.IsOptimisticLockingException(exception)
					|| dialect.IsDuplicateKeyException(exception))
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
					dialect.QuoteForTableName(sagasIndexTableName),		//< 0
					dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
					dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 2
					dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), //< 3
					dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 4
					string.Join(", ", tuples)							//< 5
				);

				foreach (var parameter in parameters)
				{
					var value = GetIndexValue(parameter.PropertyValue);
					var values = value == null ? null : ArraysEnabledFor(dialect)
						? (object)GetIndexValues(parameter.PropertyValue)?.ToArray()
						: GetConcatenatedIndexValues(GetIndexValues(parameter.PropertyValue));
					var valuesDbType = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

					command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, value);
					command.AddParameter(dialect.EscapeParameter(parameter.PropertyValuesParameter), valuesDbType, values);
				}

				command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

				try
				{
					using (var reader = command.ExecuteReader())
					{
						existingKeys = reader.AsEnumerable<string>(SAGAINDEX_KEY_COLUMN).ToArray();
					}
				}
				catch (DbException exception) when (
					dialect.IsOptimisticLockingException(exception)
					|| dialect.IsDuplicateKeyException(exception))
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
					dialect.QuoteForTableName(sagasIndexTableName),		//< 0
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
				catch (DbException exception) when (
					dialect.IsOptimisticLockingException(exception)
					|| dialect.IsDuplicateKeyException(exception))
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

			var idxTbl = dialect.QuoteForTableName(sagasIndexTableName);
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
					dialect.QuoteForTableName(sagasIndexTableName),      //< 0
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
				catch (DbException exception) when (
					dialect.IsOptimisticLockingException(exception)
					|| dialect.IsDuplicateKeyException(exception))
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
						dialect.QuoteForTableName(sagasIndexTableName),		//< 0
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
					var values = ArraysEnabledFor(dialect)
						? (object)GetIndexValues(propertiesToIndex[key])?.ToArray()
						: GetConcatenatedIndexValues(GetIndexValues(propertiesToIndex[key]));
					var valuesDbType = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), DbType.String, key);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), DbType.String, value);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN), valuesDbType, values);

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception) when (
						dialect.IsOptimisticLockingException(exception)
						|| dialect.IsDuplicateKeyException(exception))
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
						dialect.QuoteForTableName(sagasIndexTableName),      //< 0
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
					catch (DbException exception) when (
						dialect.IsOptimisticLockingException(exception)
						|| dialect.IsDuplicateKeyException(exception))
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
						dialect.QuoteForTableName(sagasIndexTableName),      //< 0
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),	//< 1
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),	//< 2
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), //< 3
						dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN),//< 4
						string.Join(", ", tuples)							//< 5
					);

					foreach (var parameter in parameters)
					{
						var value = GetIndexValue(parameter.PropertyValue);
						var values = ArraysEnabledFor(dialect)
							? (object)GetIndexValues(parameter.PropertyValue)?.ToArray()
							: GetConcatenatedIndexValues(GetIndexValues(parameter.PropertyValue));
						var valuesDbType = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

						command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, value);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValuesParameter), valuesDbType, values);
					}

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

					try
					{
						command.ExecuteNonQuery();
					}
					catch (DbException exception) when (
						dialect.IsOptimisticLockingException(exception)
						|| dialect.IsDuplicateKeyException(exception))
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

		public override void Delete(ISagaData sagaData)
		{
			using (var scope = Manager.GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						@"DELETE FROM {0} WHERE {1} = {2} AND {3} = {4};",
						dialect.QuoteForTableName(sagasTableName),
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
						dialect.QuoteForTableName(sagasIndexTableName),
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
			if (UseSagaLocking)
			{
				return UseNoWaitSagaLocking
					? $"{dialect.SelectForUpdateClause} {dialect.SelectForNoWaitClause}"
					: dialect.SelectForUpdateClause;
			}

			return string.Empty;
		}

		private bool ArraysEnabledFor(SqlDialect dialect)
		{
			return UseSqlArrays && dialect.SupportsArrayTypes;
		}

		private IDictionary<string, object> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
		{
			return sagaDataPropertyPathsToIndex
				.ToDictionary(x => x, x => Reflect.Value(sagaData, x))
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

		protected override string Fetch<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage)
		{
			using (var scope = Manager.GetScope(autocomplete: true))
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var sagaType = GetSagaTypeName(typeof(TSagaData));

				if (UseSagaLocking)
				{
					if (!dialect.SupportsSelectForUpdate)
						throw new InvalidOperationException($"You can't use saga locking for a Dialect {dialect.GetType()} that does not supports Select For Update.");

					if (UseNoWaitSagaLocking && !dialect.SupportsSelectForWithNoWait)
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
							dialect.QuoteForTableName(sagasTableName),
							dialect.QuoteForColumnName(SAGA_ID_COLUMN),
							idParam,
							dialect.QuoteForColumnName(SAGA_TYPE_COLUMN),
							sagaTypeParam,
							GetSagaLockingClause(dialect)
						);
						command.AddParameter(sagaTypeParam, sagaType);
						command.AddParameter(idParam, id);
					}
					else
					{
						var dataCol = dialect.QuoteForColumnName(SAGA_DATA_COLUMN);
						var sagaTblName = dialect.QuoteForTableName(sagasTableName);
						var sagaTypeCol = dialect.QuoteForColumnName(SAGA_TYPE_COLUMN);
						var sagaTypeParam = dialect.EscapeParameter(SAGA_TYPE_COLUMN);
						var indexTblName = dialect.QuoteForTableName(sagasIndexTableName);
						var sagaIdCol = dialect.QuoteForColumnName(SAGA_ID_COLUMN);
						var indexIdCol = dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN);
						var indexKeyCol = dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN);
						var indexKeyParam = dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN);
						var indexValueCol = dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN);
						var indexValueParm = dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN);
						var indexValuesCol = dialect.QuoteForColumnName(SAGAINDEX_VALUES_COLUMN);
						var indexValuesParm = dialect.EscapeParameter(SAGAINDEX_VALUES_COLUMN);
						var forUpdate = GetSagaLockingClause(dialect);
						var valuesPredicate = ArraysEnabledFor(dialect)
							? dialect.FormatArrayAny($"i.{indexValuesCol}", indexValuesParm)
							: $"(i.{indexValuesCol} LIKE ('%' || {indexValuesParm} || '%'))";

						command.CommandText = $@"
							SELECT s.{dataCol}
							FROM {sagaTblName} s
							JOIN {indexTblName} i on s.{sagaIdCol} = i.{indexIdCol}
							WHERE s.{sagaTypeCol} = {sagaTypeParam}
								AND i.{indexKeyCol} = {indexKeyParam}
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
						var values = value == null ? DBNull.Value : ArraysEnabledFor(dialect)
							? (object)(new[] { value })
							: GetConcatenatedIndexValues(new[] { value });
						var valuesDbType = ArraysEnabledFor(dialect) ? DbType.Object : DbType.String;

						command.AddParameter(indexKeyParam, sagaDataPropertyPath);
						command.AddParameter(sagaTypeParam, sagaType);
						command.AddParameter(indexValueParm, DbType.String, value);
						command.AddParameter(indexValuesParm, valuesDbType, values);
					}


					try
					{
						log.Debug("Finding saga of type {0} with {1} = {2}", sagaType, sagaDataPropertyPath, fieldFromMessage);
						return (string)command.ExecuteScalar();
					}
					catch (DbException ex)
					{
						// When in no-wait saga-locking mode, inspect
						// exception and rethrow ex as SagaLockedException.
						if (UseSagaLocking && UseNoWaitSagaLocking)
						{
							if (dialect.IsSelectForNoWaitLockingException(ex))
								throw new AdoNetSagaLockedException(ex);
						}

						throw;
					}
				}
			}
		}
	}
}

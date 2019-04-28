﻿using System;
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
		private readonly bool useSagaLocking;
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
		public AdoNetSagaPersister(AdoNetUnitOfWorkManager manager, string sagaTableName, string sagaIndexTableName, bool useSagaLocking)
		{
			this.manager = manager;
			this.sagaTableName = sagaTableName;
			this.sagaIndexTableName = sagaIndexTableName;
			this.idPropertyName = Reflect.Path<ISagaData>(x => x.Id);
			this.useSagaLocking = useSagaLocking;
		}

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
								new AdoNetColumn() { Name = SAGA_TYPE_COLUMN, DbType = DbType.StringFixedLength, Length = 40 },
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
								new AdoNetColumn() { Name = SAGAINDEX_TYPE_COLUMN, DbType = DbType.StringFixedLength, Length = 40 },
								new AdoNetColumn() { Name = SAGAINDEX_KEY_COLUMN, DbType = DbType.StringFixedLength, Length = 200  },
								new AdoNetColumn() { Name = SAGAINDEX_VALUE_COLUMN, DbType = DbType.StringFixedLength, Length = 200 },
								new AdoNetColumn() { Name = SAGAINDEX_ID_COLUMN, DbType = DbType.Guid }
							},
							PrimaryKey = new[] { SAGAINDEX_KEY_COLUMN, SAGAINDEX_VALUE_COLUMN, SAGAINDEX_ID_COLUMN },
							Indexes = new []
							{
								new AdoNetIndex() { Name = "ix_saga_id", Columns = new[] { SAGAINDEX_ID_COLUMN } },
								new AdoNetIndex() { Name = "ix_sagaindexes_id_type_key", Columns = new[] { SAGAINDEX_ID_COLUMN, SAGAINDEX_TYPE_COLUMN, SAGAINDEX_KEY_COLUMN } }
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
					CreateIndex(sagaData, scope, propertiesToIndex);
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
					CreateIndex(sagaData, scope, propertiesToIndex);
				}

				scope.Complete();
			}
		}

		private void CreateIndex(ISagaData sagaData, AdoNetUnitOfWorkScope scope, IEnumerable<KeyValuePair<string, string>> propertiesToIndex)
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

			var values = string.Join(", ", parameters.Select(p => string.Format("({0}, {1}, {2}, {3})",
						dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN),
						dialect.EscapeParameter(p.PropertyNameParameter),
						dialect.EscapeParameter(p.PropertyValueParameter),
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN))));

			if (dialect.SupportsTableExpressions)
			{
				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						"WITH rebusexistingkeys AS " +
							"(INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5} " +
							"ON CONFLICT ({2}, {1}, {4}) DO UPDATE SET {3} = excluded.{3} " +
							"RETURNING {2}) " +
						"DELETE FROM {0} " +
						"WHERE {4} = {6} AND {2} NOT IN " +
						"(SELECT {2} FROM rebusexistingkeys)",
						dialect.QuoteForTableName(sagaIndexTableName),
						dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),
						values,
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN));

					foreach (var parameter in parameters)
					{
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, parameter.PropertyValue);
					}

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);
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
			else
			{
				var existingKeys = new List<string>();

				using (var command = connection.CreateCommand())
				{
					command.CommandText = string.Format(
						"INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES {5} " +
							"ON CONFLICT ({2}, {1}, {4}) DO UPDATE SET {3} = excluded.{3} " +
							"RETURNING {2}",
						dialect.QuoteForTableName(sagaIndexTableName),
						dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),
						values);

					foreach (var parameter in parameters)
					{
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyNameParameter), DbType.String, parameter.PropertyName);
						command.AddParameter(dialect.EscapeParameter(parameter.PropertyValueParameter), DbType.String, parameter.PropertyValue);
					}

					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), DbType.String, sagaTypeName);
					command.AddParameter(dialect.EscapeParameter(SAGAINDEX_ID_COLUMN), DbType.Guid, sagaData.Id);

					try
					{
						using (var reader = command.ExecuteReader())
						{
							while (reader.Read())
							{
								existingKeys.Add((string)reader[SAGAINDEX_KEY_COLUMN]);
							}
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
						"WHERE {1} = {2} AND {3} NOT IN " +
						"({4})",
	
						dialect.QuoteForTableName(sagaIndexTableName),
						dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),
						dialect.EscapeParameter(SAGAINDEX_ID_COLUMN),
						dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN),
						string.Join(", ", existingKeys.Select(k => dialect.EscapeParameter($"k{idx++}"))));

					for(int i = 0; i < existingKeys.Count; i++)
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

				if (useSagaLocking && !dialect.SupportsSelectForUpdate)
					throw new InvalidOperationException($"You can't use saga locking for a Dialect {dialect.GetType()} that is not supporting Select For Update");

				using (var command = connection.CreateCommand())
				{
					if (sagaDataPropertyPath == idPropertyName)
					{
						var id = (fieldFromMessage is Guid) ? (Guid)fieldFromMessage : Guid.Parse(fieldFromMessage.ToString());
						var parameter = dialect.EscapeParameter("value");
						var sagaType = dialect.EscapeParameter(SAGA_TYPE_COLUMN);

						command.CommandText = string.Format(
							@"SELECT s.{0} FROM {1} s WHERE s.{2} = {3} AND s.{4} = {5} {6}",
							dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
							dialect.QuoteForTableName(sagaTableName),
							dialect.QuoteForColumnName(SAGA_TYPE_COLUMN),
							sagaType,
							dialect.QuoteForColumnName(SAGA_ID_COLUMN),
							parameter,
							useSagaLocking ? dialect.ParameterSelectForUpdate : string.Empty
						);
						command.AddParameter(sagaType, GetSagaTypeName(typeof(TSagaData)));
						command.AddParameter(parameter, id);
					}
					else
					{
						command.CommandText = string.Format(
							@"SELECT s.{0} " +
							@"FROM {1} s " +
							@"JOIN {2} i on s.{3} = i.{4} " +
							@"WHERE i.{5} = {6} AND i.{7} = {8} AND i.{9} = {10} {11}",
							dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
							dialect.QuoteForTableName(sagaTableName),
							dialect.QuoteForTableName(sagaIndexTableName),
							dialect.QuoteForColumnName(SAGA_ID_COLUMN), dialect.QuoteForColumnName(SAGAINDEX_ID_COLUMN),
							dialect.QuoteForColumnName(SAGAINDEX_TYPE_COLUMN), dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN),
							dialect.QuoteForColumnName(SAGAINDEX_KEY_COLUMN), dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN),
							dialect.QuoteForColumnName(SAGAINDEX_VALUE_COLUMN), dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN),
							useSagaLocking ? dialect.ParameterSelectForUpdate : string.Empty
						);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_KEY_COLUMN), sagaDataPropertyPath);
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_TYPE_COLUMN), GetSagaTypeName(typeof(TSagaData)));
						command.AddParameter(dialect.EscapeParameter(SAGAINDEX_VALUE_COLUMN), (fieldFromMessage ?? "").ToString());
					}

					var value = (string)command.ExecuteScalar();

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

		List<KeyValuePair<string, string>> GetPropertiesToIndex(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
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
				.ToList();
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
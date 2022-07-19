#if true
using System;
using System.Data;
using System.Linq;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Globalization;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Rebus.Logging;
using Rebus.AdoNet.Schema;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Implements a saga persister for Rebus that stores sagas using an AdoNet provider.
	/// This is an advanced implementation using single-table scheme for saga & indexes.
	/// </summary>
	public class AdoNetSagaPersisterAdvanced : AdoNetSagaPersister, IStoreSagaData, AdoNetSagaPersisterFluentConfigurer, ICanUpdateMultipleSagaDatasAtomically
	{
		private const int MaximumSagaDataTypeNameLength = 80;
		private const string SAGA_ID_COLUMN = "id";
		private const string SAGA_TYPE_COLUMN = "saga_type";
		private const string SAGA_DATA_COLUMN = "data";
		private const string SAGA_REVISION_COLUMN = "revision";
		private const string SAGA_CORRELATIONS_COLUMN = "correlations";
		private static ILog log;

		// TODO?: Maybe we should implement our own micro-serialization logic, so we can control actual conversions.
		//		  I am thinking for example on issues with preccision on decimals, etc. (pruiz)
		private static readonly JsonSerializerSettings IndexSerializerSettings = new JsonSerializerSettings {
			Culture =  CultureInfo.InvariantCulture,
			TypeNameHandling = TypeNameHandling.None, // TODO: Make it configurable?
			DateFormatHandling = DateFormatHandling.IsoDateFormat, // TODO: Make it configurable?
			Converters = new List<JsonConverter>() {
				new StringEnumConverter()
			}
		};

		private readonly AdoNetUnitOfWorkManager manager;
		private readonly string sagasTableName;
		private readonly string idPropertyName;

		static AdoNetSagaPersisterAdvanced()
		{
			RebusLoggerFactory.Changed += f => log = f.GetCurrentClassLogger();
		}

		/// <summary>
		/// Constructs the persister with the ability to create connections to database using the specified connection string.
		/// This also means that the persister will manage the connection by itself, closing it when it has stopped using it.
		/// </summary>
		public AdoNetSagaPersisterAdvanced(AdoNetUnitOfWorkManager manager, string sagasTableName)
			: base(manager)
		{
			this.manager = manager;
			this.sagasTableName = sagasTableName;
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
			using (var uow = manager.Create(autonomous: true))
			using (var scope = (uow as AdoNetUnitOfWork).GetScope())
			{
				var dialect = scope.Dialect;
				var connection = scope.Connection;
				var tableNames = scope.GetTableNames();

				// bail out if there's already a table in the database with one of the names
				var sagaTableIsAlreadyCreated = tableNames.Contains(sagasTableName, StringComparer.InvariantCultureIgnoreCase);

				if (sagaTableIsAlreadyCreated)
				{
					log.Debug("Table '{0}' already exists.", sagasTableName);
					return this;
				}

				if (UseSqlArrays /* && !dialect.SupportsArrayTypes*/)
				{
					throw new ApplicationException("Enabled UseSqlArraysForCorrelationIndexes AdoNetSagaPersister selecte does not support arrays?!");
					//throw new ApplicationException("Enabled UseSqlArraysForCorrelationIndexes but underlaying database does not support arrays?!");
				}

				var indexes = new[] {
					new AdoNetIndex() {
						Name = $"ix_{sagasTableName}_{SAGA_ID_COLUMN}_{SAGA_TYPE_COLUMN}",
						Columns = new[] { SAGA_ID_COLUMN, SAGA_TYPE_COLUMN }
					},
				}.ToList();

				if (dialect.SupportsGinIndexes)
				{
					if (dialect.SupportsMultiColumnGinIndexes)
					{
						indexes.Add(new AdoNetIndex() {
							Name = $"ix_{sagasTableName}_{SAGA_TYPE_COLUMN}_{SAGA_CORRELATIONS_COLUMN}",
							Columns = new[] { SAGA_TYPE_COLUMN, SAGA_CORRELATIONS_COLUMN },
							Kind = AdoNetIndex.Kinds.GIN,
						});
					}
					else
					{
						indexes.Add(new AdoNetIndex() {
							Name = $"ix_{sagasTableName}_{SAGA_CORRELATIONS_COLUMN}",
							Columns = new[] { SAGA_CORRELATIONS_COLUMN },
							Kind = AdoNetIndex.Kinds.GIN,
						});
					}
				}

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
								new AdoNetColumn() { Name = SAGA_DATA_COLUMN, DbType = DbType.String, Length = 1073741823 },
								new AdoNetColumn() { Name = SAGA_CORRELATIONS_COLUMN, DbType = DbType.Object,  }
							},
							PrimaryKey = new[] { SAGA_ID_COLUMN },
							Indexes = indexes
						}
					);

					log.Info("Table '{0}' do not exists - it will be created now using:\n{1}", sagasTableName, command.CommandText);

					command.ExecuteNonQuery();
				}

				scope.Complete();
				log.Info("Table '{0}' created", sagasTableName);
			}

			return this;
		}

		#endregion

		protected string SerializeCorrelations(IDictionary<string, object> sagaData)
		{
			return JsonConvert.SerializeObject(sagaData, Formatting.Indented, IndexSerializerSettings);
		}
		public override void Insert(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = manager.GetScope())
			using (var command = scope.Connection.CreateCommand())
			{
				var dialect = scope.Dialect;
				var sagaTypeName = GetSagaTypeName(sagaData.GetType());
				var propertiesToIndex = GetCorrelationItems(sagaData, sagaDataPropertyPathsToIndex);
				var correlations = propertiesToIndex.Any() ? SerializeCorrelations(propertiesToIndex) : "{}";

				// next insert the saga
				command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
				command.AddParameter(dialect.EscapeParameter(SAGA_TYPE_COLUMN), sagaTypeName);
				command.AddParameter(dialect.EscapeParameter(SAGA_REVISION_COLUMN), ++sagaData.Revision);
				command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), Serialize(sagaData));
				command.AddParameter(dialect.EscapeParameter(SAGA_CORRELATIONS_COLUMN), correlations);

				command.CommandText = string.Format(
					@"insert into {0} ({1}, {2}, {3}, {4}, {5}) values ({6}, {7}, {8}, {9}, {10});",
					dialect.QuoteForTableName(sagasTableName),
					dialect.QuoteForColumnName(SAGA_ID_COLUMN),
					dialect.QuoteForColumnName(SAGA_TYPE_COLUMN),
					dialect.QuoteForColumnName(SAGA_REVISION_COLUMN),
					dialect.QuoteForColumnName(SAGA_DATA_COLUMN),
					dialect.QuoteForColumnName(SAGA_CORRELATIONS_COLUMN),
					dialect.EscapeParameter(SAGA_ID_COLUMN),
					dialect.EscapeParameter(SAGA_TYPE_COLUMN),
					dialect.EscapeParameter(SAGA_REVISION_COLUMN),
					dialect.EscapeParameter(SAGA_DATA_COLUMN),
					dialect.Cast(dialect.EscapeParameter(SAGA_CORRELATIONS_COLUMN), DbType.Object)
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

				scope.Complete();
			}
		}

		public override void Update(ISagaData sagaData, string[] sagaDataPropertyPathsToIndex)
		{
			using (var scope = manager.GetScope())
			using (var command = scope.Connection.CreateCommand())
			{
				var dialect = scope.Dialect;
				var items = GetCorrelationItems(sagaData, sagaDataPropertyPathsToIndex);
				var correlations = items.Any() ? SerializeCorrelations(items) : "{}";

				// next, update or insert the saga
				command.AddParameter(dialect.EscapeParameter(SAGA_ID_COLUMN), sagaData.Id);
				command.AddParameter(dialect.EscapeParameter("current_revision"), sagaData.Revision);
				command.AddParameter(dialect.EscapeParameter("next_revision"), ++sagaData.Revision);
				command.AddParameter(dialect.EscapeParameter(SAGA_DATA_COLUMN), Serialize(sagaData));
				command.AddParameter(dialect.EscapeParameter(SAGA_CORRELATIONS_COLUMN), correlations);

				command.CommandText = string.Format(
					@"UPDATE {0} SET {1} = {2}, {3} = {4}, {5} = {6} " +
					@"WHERE {7} = {8} AND {9} = {10};",
					dialect.QuoteForTableName(sagasTableName),
					dialect.QuoteForColumnName(SAGA_DATA_COLUMN), dialect.EscapeParameter(SAGA_DATA_COLUMN),
					dialect.QuoteForColumnName(SAGA_REVISION_COLUMN), dialect.EscapeParameter("next_revision"),
					dialect.QuoteForColumnName(SAGA_CORRELATIONS_COLUMN), dialect.Cast(dialect.EscapeParameter(SAGA_CORRELATIONS_COLUMN), DbType.Object),
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

				scope.Complete();
			}
		}

		public override void Delete(ISagaData sagaData)
		{
			using (var scope = manager.GetScope())
			using (var command = scope.Connection.CreateCommand())
			{
				var dialect = scope.Dialect;

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

				scope.Complete();
			}
		}

		#region Fetch
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

		private bool ShouldIndexValue(object value)
		{
			if (IndexNullProperties)
				return true;

			if (value == null) return false;
			if (value is string) return true;
			if ((value is IEnumerable) && !(value as IEnumerable).Cast<object>().Any()) return false;

			return true;
		}

		private IDictionary<string, object> GetCorrelationItems(ISagaData sagaData, IEnumerable<string> sagaDataPropertyPathsToIndex)
		{
			return sagaDataPropertyPathsToIndex
				   .Select(x => new { Key = x, Value = Reflect.Value(sagaData, x) })
				   .Where(ShouldIndexValue)
				   .ToDictionary(x => x.Key, x => x.Value);
		}

		private static void Validate(object correlation)
		{
			var type = correlation?.GetType();
			if (type == null)
				return;

			if (type.IsArray)
			{
				if (type.GetArrayRank() > 1)
					throw new ArgumentOutOfRangeException("Multidimensional arrays are not supported.");

				type = type.GetElementType();
			}

			type = Nullable.GetUnderlyingType(type) ?? type;
			if (type.IsPrimitive || type.IsEnum)
				return;

			if (type == typeof(string) || type == typeof(Guid) || type == typeof(DateTime)
				|| type == typeof(decimal) || type == typeof(TimeSpan))
			{
				return;
			}

			throw new NotSupportedException($"Type {type.Name} is not supported as a correlation value.");
		}

		protected override string Fetch<TSagaData>(string sagaDataPropertyPath, object fieldFromMessage)
		{
			using (var scope = manager.GetScope(autocomplete: true))
			using (var command = scope.Connection.CreateCommand())
			{
				var dialect = scope.Dialect;
				var sagaType = GetSagaTypeName(typeof(TSagaData));

				if (UseSagaLocking)
				{
					if (!dialect.SupportsSelectForUpdate)
						throw new InvalidOperationException(
							$"You can't use saga locking for a Dialect {dialect.GetType()} that does not supports Select For Update.");

					if (UseNoWaitSagaLocking && !dialect.SupportsSelectForWithNoWait)
						throw new InvalidOperationException(
							$"You can't use saga locking with no-wait for a Dialect {dialect.GetType()} that does not supports no-wait clause.");
				}

				if (sagaDataPropertyPath == idPropertyName)
				{
					var id = (fieldFromMessage is Guid)
						? (Guid)fieldFromMessage
						: Guid.Parse(fieldFromMessage.ToString());
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
					Validate(correlation: fieldFromMessage);

					var dataCol = dialect.QuoteForColumnName(SAGA_DATA_COLUMN);
					var sagaTblName = dialect.QuoteForTableName(sagasTableName);
					var sagaTypeCol = dialect.QuoteForColumnName(SAGA_TYPE_COLUMN);
					var sagaTypeParam = dialect.EscapeParameter(SAGA_TYPE_COLUMN);
					var sagaCorrelationsCol = dialect.QuoteForColumnName(SAGA_CORRELATIONS_COLUMN);
					var sagaCorrelationsValueParam = dialect.EscapeParameter("value");
					var sagaCorrelationsValuesParam = dialect.EscapeParameter("values");
					var forUpdate = GetSagaLockingClause(dialect);

					command.CommandText = $@"
						SELECT s.{dataCol}
						FROM {sagaTblName} s
						WHERE s.{sagaTypeCol} = {sagaTypeParam}
							AND (
							    s.{sagaCorrelationsCol} @> {dialect.Cast(sagaCorrelationsValueParam, DbType.Object)}
							    OR
							    s.{sagaCorrelationsCol} @> {dialect.Cast(sagaCorrelationsValuesParam, DbType.Object)}
							  )
						{forUpdate};".Replace("\t", "");

					var value = SerializeCorrelations(new Dictionary<string, object>() { { sagaDataPropertyPath, fieldFromMessage } });
					var values = SerializeCorrelations(new Dictionary<string, object>() { { sagaDataPropertyPath, new[] { fieldFromMessage } } });

					command.AddParameter(sagaTypeParam, sagaType);
					command.AddParameter(sagaCorrelationsValueParam, DbType.String, value);
					command.AddParameter(sagaCorrelationsValuesParam, DbType.String, values);
				}

				try
				{
					log.Debug("Finding saga of type {0} with {1} = {2}\n{3}", sagaType, sagaDataPropertyPath,
						fieldFromMessage, command.CommandText);
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

		#endregion
	}
}
#endif

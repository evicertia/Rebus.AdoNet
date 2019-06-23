using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;

using Common.Logging;

using NUnit.Framework;

using Rebus.Configuration;
using Rebus.AdoNet.Dialects;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet
{
	public abstract class DatabaseFixtureBase : FixtureBase, IDetermineMessageOwnership
	{
		private static readonly ILog _Log = LogManager.GetLogger<DatabaseFixtureBase>();

		private const string CONNECTION_STRING = @"Data Source={0};Version=3;New=True;";

		protected string ConnectionString { get; }
		protected string ProviderName { get; }
		protected DbProviderFactory Factory { get; }
		protected SqlDialect Dialect { get; }

		protected DatabaseFixtureBase(string provider, string connectionString)
		{
			ProviderName = provider;
			ConnectionString = connectionString;
			Factory = DbProviderFactories.GetFactory(ProviderName);
			Dialect = GetDialect();

			if (Dialect == null) throw new InvalidOperationException($"No valid dialect detected for: {ProviderName}");
		}

		protected static string GetSqliteProviderName()
		{
			switch (Environment.OSVersion.Platform)
			{
				case PlatformID.Unix: return "System.Data.SQLite.Mac"; //< OSX can report Unix too.
				case PlatformID.MacOSX: return "System.Data.SQLite.Mac";
				default: return "System.Data.SQLite";
			}
		}

		private static string GetSqliteConnectionString()
		{
			var dbfile = AssemblyFixture.TrackDisposable(new TempFile());
			File.Delete(dbfile.Path);
			_Log.DebugFormat("Using temporal file: {0}", dbfile.Path);
			return string.Format(CONNECTION_STRING, dbfile.Path);
		}

		private static string GetPostgresConnectionString()
		{
			// AppVeyor's default credentials..
			return "User ID=postgres;Password=Password12!;Host=localhost;Port=5432;Database=test;Pooling=true;";
		}

		public static IEnumerable<object[]> ConnectionSources()
		{
			yield return new[] { GetSqliteProviderName(), GetSqliteConnectionString() };
			yield return new[] { "Npgsql", GetPostgresConnectionString() };
		}

		private SqlDialect GetDialect()
		{
			using (var connection = Factory.CreateConnection())
			{
				connection.ConnectionString = ConnectionString;
				connection.Open();

				return SqlDialect.GetDialectFor(connection);
			}
		}

		protected void ExecuteCommand(string commandText)
		{
			using (var connection = Factory.CreateConnection())
			{
				connection.ConnectionString = ConnectionString;
				connection.Open();
				connection.ExecuteCommand(commandText);
			}
		}

		protected object ExecuteScalar(string commandText)
		{
			using (var connection = Factory.CreateConnection())
			{
				connection.ConnectionString = ConnectionString;
				connection.Open();
				return connection.ExecuteScalar(commandText);
			}
		}

		protected IEnumerable<string> GetTableNames()
		{
			using (var connection = Factory.CreateConnection())
			{
				connection.ConnectionString = ConnectionString;
				connection.Open();
				return Dialect.GetTableNames(connection);
			}
		}

		protected void DropTable(string tableName)
		{
			if (!GetTableNames().Contains(tableName, StringComparer.InvariantCultureIgnoreCase)) return;

			ExecuteCommand(string.Format(@"DROP TABLE ""{0}""", tableName));
		}

		protected void DeleteRows(string tableName)
		{
			if (!GetTableNames().Contains(tableName, StringComparer.InvariantCultureIgnoreCase)) return;

			ExecuteCommand(string.Format(@"DELETE FROM ""{0}""", tableName));
		}

		public string GetEndpointFor(Type messageType)
		{
			return null;
		}

#if false
		protected IStartableBus CreateBus(BuiltinContainerAdapter adapter, string inputQueueName)
		{
			var bus = Configure.With(adapter)
							   .Transport(t => t.UseMsmq(inputQueueName, ErrorQueueName))
							   .MessageOwnership(d => d.Use(this))
							   .Behavior(b => b.HandleMessagesInsideTransactionScope())
							   .Subscriptions(
								   s =>
								   s.StoreInPostgreSql(ConnectionString, "RebusSubscriptions")
									.EnsureTableIsCreated())
							   .Sagas(
								   s =>
								   s.StoreInPostgreSql(ConnectionString, SagaTableName, SagaIndexTableName)
									.EnsureTablesAreCreated())
							   .CreateBus();
			return bus;
		}
#endif
	}
}
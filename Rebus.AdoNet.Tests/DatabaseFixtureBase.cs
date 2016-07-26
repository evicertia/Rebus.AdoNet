using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;

using NUnit.Framework;
using Common.Logging;

using Rebus.Configuration;
using Rebus.AdoNet.Dialects;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet
{
	public abstract class DatabaseFixtureBase : IDetermineMessageOwnership
	{
		private const string ErrorQueueName = "error";
		protected const string SagaTableName = "Sagas";
		protected const string SagaIndexTableName = "SagasIndex";
		
		protected string ConnectionString { get; }
		protected string ProviderName { get; }
		protected DbProviderFactory Factory { get; }
		protected SqlDialect Dialect { get; }

		protected DatabaseFixtureBase(string connectionString, string providerName)
		{
			ConnectionString = connectionString;
			ProviderName = providerName;
			Factory = DbProviderFactories.GetFactory(providerName);
			Dialect = GetDialect();

			if (Dialect == null) throw new InvalidOperationException($"No valid dialect detected for: {providerName}");
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

		protected void DropSagaTables()
		{
			try
			{
				DropTable(SagaTableName);
			}
			catch
			{
			}

			try
			{
				DropTable(SagaIndexTableName);
			}
			catch
			{
			}
		}

		protected virtual void OnSetUp()
		{
		}

		protected virtual void DoTearDown()
		{
		}

		[SetUp]
		public void SetUp()
		{
			//TimeMachine.Reset();
			OnSetUp();
		}

		[TearDown]
		public void TearDown()
		{
			DoTearDown();
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
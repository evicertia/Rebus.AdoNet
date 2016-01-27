using System;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Transactions;

using IsolationLevel = System.Data.IsolationLevel;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Base class for AdoNet storage implementations.
	/// </summary>
	public abstract class AdoNetStorage
	{
		protected DbProviderFactory _factory;
		protected Func<ConnectionHolder> getConnection;
		protected Action<ConnectionHolder> commitAction;
		protected Action<ConnectionHolder> rollbackAction;
		protected Action<ConnectionHolder> releaseConnection;

		protected AdoNetStorage(Func<ConnectionHolder> connectionFactoryMethod)
		{
			getConnection = connectionFactoryMethod;
			commitAction = h => { };
			rollbackAction = h => { };
			releaseConnection = c => { };
		}

		protected AdoNetStorage(string connectionString, string providerName)
		{
			_factory = DbProviderFactories.GetFactory(providerName); 
			getConnection = () => CreateConnection(_factory, connectionString);
			commitAction = h => h.Commit();
			rollbackAction = h => h.Rollback();
			releaseConnection = h => h.Dispose();
		}

		private static ConnectionHolder CreateConnection(DbProviderFactory factory, string connectionString)
		{
			var connection = factory.CreateConnection();
			connection.ConnectionString = connectionString;

			connection.Open();

			if (Transaction.Current == null)
			{
				var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

				return ConnectionHolder.ForTransactionalWork(connection, transaction);
			}

			return ConnectionHolder.ForNonTransactionalWork(connection);
		}
	}
}
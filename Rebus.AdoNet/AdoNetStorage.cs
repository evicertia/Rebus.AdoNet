using System;
using System.Collections.Concurrent;
using System.Data;
using System.Data.Common;
using System.Configuration;
using System.Transactions;

using Rebus.AdoNet.Dialects;

using IsolationLevel = System.Data.IsolationLevel;

namespace Rebus.AdoNet
{
	/// <summary>
	/// Base class for AdoNet storage implementations.
	/// </summary>
	public abstract class AdoNetStorage
	{
		private static readonly ConcurrentDictionary<string, SqlDialect> _dialects = new ConcurrentDictionary<string, SqlDialect>();
		protected readonly DbProviderFactory _factory;
		protected readonly Func<ConnectionHolder> getConnection;
		protected readonly Action<ConnectionHolder> commitAction;
		protected readonly Action<ConnectionHolder> rollbackAction;
		protected readonly Action<ConnectionHolder> releaseConnection;

		protected AdoNetStorage(Func<ConnectionHolder> connectionFactoryMethod)
		{
			getConnection = connectionFactoryMethod;
			commitAction = x => { };
			rollbackAction = x => { };
			releaseConnection = x => { };
		}

		protected AdoNetStorage(string connectionString, string providerName)
		{
			_factory = DbProviderFactories.GetFactory(providerName);
			//getConnection = () => GetConnection(_factory, connectionString);
			getConnection = () => CreateConnection(_factory, connectionString);
			commitAction = x => x.Commit();
			rollbackAction = x => x.Abort();
			releaseConnection = x => x.Dispose();
		}

		private static ConnectionHolder CreateConnection(DbProviderFactory factory, string connectionString)
		{
			var connection = factory.OpenConnection(connectionString);
			var dialect = _dialects.GetOrAdd(connectionString, x => SqlDialect.GetDialectFor(connection));
			if (dialect == null) throw new InvalidOperationException($"Unable to guess dialect for: {connectionString}");

			if (Transaction.Current == null)
			{
				var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);
				return ConnectionHolder.ForTransactionalWork(connection, dialect, transaction);
			}

			return ConnectionHolder.ForNonTransactionalWork(connection, dialect);
		}
#if false
		private static ConnectionHolder GetConnection(DbProviderFactory factory, string connectionString)
		{
			// If there no established message context, just return an isolated connection.
			if (!MessageContext.HasCurrent) return CreateConnection(factory, connectionString);


		}
#endif
	}
}
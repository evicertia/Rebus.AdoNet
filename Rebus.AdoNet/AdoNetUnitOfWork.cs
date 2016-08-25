using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;

using Rebus;
using Rebus.Bus;
using Rebus.Logging;

namespace Rebus.AdoNet
{
	internal class AdoNetUnitOfWork : IUnitOfWork
	{
		private static ILog _log;
		private IDbConnection _connection;
		private IDbTransaction _transaction;

		public IDbConnection Connection => _connection;

		static AdoNetUnitOfWork()
		{
			RebusLoggerFactory.Changed += f => _log = f.GetCurrentClassLogger();

		}

		public AdoNetUnitOfWork()
		{
			_log.Debug("Created new instance");
		}

		public AdoNetUnitOfWork(IDbConnection connection)
		{
			if (connection == null) throw new ArgumentNullException(nameof(connection));

			_connection = connection;
			_transaction = _connection.BeginTransaction(IsolationLevel.ReadCommitted); //< We may require 'Serializable' as our default.

			_log.Debug("Created new instance with connection {0} and transaction {1}", _connection.GetHashCode(), _transaction.GetHashCode());
		}

		public void Use(IDbConnection connection)
		{
			if (connection == null) throw new ArgumentNullException(nameof(connection));
			if (_connection != null) throw new InvalidOperationException("UnitOfWork already using a connection?!");

			_connection = connection;
			_transaction = _connection.BeginTransaction(IsolationLevel.ReadCommitted); //< We may require 'Serializable' as our default.

			_log.Debug("Using connection {0} and transaction {1}", _connection.GetHashCode(), _transaction.GetHashCode());

		}

		public void Abort()
		{
			if (_transaction == null) throw new InvalidOperationException("UnitOfWork has no active transaction?!");

			_log.Debug("Rolling back transaction: {0}...", _transaction.GetHashCode());
			_transaction.Rollback();
			_log.Debug("Rolled back transaction: {0}...", _transaction.GetHashCode());
		}

		public void Commit()
		{
			if (_transaction == null) throw new InvalidOperationException("UnitOfWork has no active transaction?!");

			_log.Debug("Committing transaction: {0}...", _transaction.GetHashCode());
			_transaction.Commit();
			_log.Debug("Committed transaction: {0}...", _transaction.GetHashCode());
		}



		#region IDisposable
		private bool _disposed = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (_disposed) return;

			if (disposing)
			{
				// dispose managed state (managed objects implemeting IDisposable).
				if (_transaction != null)
				{
					_log.Debug("Disposing transaction: {0}...", _transaction.GetHashCode());
					_transaction.Dispose();
					_log.Debug("Disposed transaction: {0}...", _transaction.GetHashCode());
					_transaction = null;
				}

				if (_connection != null)
				{
					_log.Debug("Disposing connection: {0}...", _connection.GetHashCode());
					_connection.Dispose();
					_log.Debug("Disposed connection: {0}...", _connection.GetHashCode());
					_connection = null;
				}
			}

			// XXX: Never throw exceptions from this point forward, log them instead.
			//		Throwing exceptions from a finalizer can crash the app.

			// free unmanaged resources (unmanaged objects), if any, and set large fields to null.
			// Then enable finalizer override found below.

			_disposed = true;
		}

#if false //< override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
		~AdoNetUnitOfWork() {
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(false); //< disposing == false, cause we are finalizing here, diposing has already been performed.
		}
#endif

		// This code added to correctly implement the disposable pattern.
		public void Dispose()
		{
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(true);

#if false //< uncomment the following line if the finalizer is overridden above.
			GC.SuppressFinalize(this);
#endif
		}
		#endregion
	}
}

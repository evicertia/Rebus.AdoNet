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
	public interface IAdoNetUnitOfWork : IUnitOfWork
	{
		AdoNetUnitOfWorkScope GetScope();
	}

	internal class AdoNetUnitOfWork : IAdoNetUnitOfWork
	{
		private static ILog _log;
		private readonly AdoNetConnectionFactory _factory;
		private Lazy<Tuple<IDbConnection, IDbTransaction>> __connection;
		private bool _aborted;
		private bool _autodispose;
		//private Queue<AdoNetUnitOfWorkScope> _scopes = new Queue<AdoNetUnitOfWorkScope>();

		private IDbConnection Connection {
			get {
				EnsureNotDisposed();
				return __connection?.Value.Item1;
			}
		}

		private IDbTransaction Transaction {
			get {
				EnsureNotDisposed();
				return __connection?.Value.Item2;
			}
		}

		/// <summary>
		/// Occurs when [on dispose].
		/// </summary>
		public event Action OnDispose = delegate { };

		static AdoNetUnitOfWork()
		{
			RebusLoggerFactory.Changed += f => _log = f.GetCurrentClassLogger();
		}

		public AdoNetUnitOfWork(AdoNetConnectionFactory factory, IMessageContext context)
		{
			if (factory == null) throw new ArgumentNullException(nameof(factory));

			_factory = factory;
			_autodispose = context == null;
			__connection = new Lazy<Tuple<IDbConnection, IDbTransaction>>(() => {
				var connection = factory.OpenConnection();
				var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted); //< We may require 'Serializable' as our default.
				_log.Debug("Created new connection {0} and transaction {1}", connection.GetHashCode(), transaction.GetHashCode());
				return Tuple.Create(connection, transaction);
			});

			_log.Debug("Created new instance for context: {0}", context?.GetHashCode());
		}

		private void EnsureNotDisposed()
		{
			if (_disposed)
			{
				throw new ObjectDisposedException(nameof(AdoNetUnitOfWork));
			}
		}
		
		public AdoNetUnitOfWorkScope GetScope()
		{
			EnsureNotDisposed();
			
			var result = new AdoNetUnitOfWorkScope(this, _factory.Dialect, Connection);

			if (_autodispose) {
				result.OnDispose +=
					() =>
					{
						if (!_aborted) Commit();
						this.Dispose();
					};
			}

			return result;
		}

		public void Abort()
		{
			EnsureNotDisposed();
			
			if (Transaction == null) throw new InvalidOperationException("UnitOfWork has no active transaction?!");

			_log.Debug("Rolling back transaction: {0}...", Transaction.GetHashCode());
			Transaction.Rollback();
			_aborted = true;
			_log.Debug("Rolled back transaction: {0}...", Transaction.GetHashCode());
		}

		public void Commit()
		{
			EnsureNotDisposed();
			
			if (Transaction == null) throw new InvalidOperationException("UnitOfWork has no active transaction?!");

			_log.Debug("Committing transaction: {0}...", Transaction.GetHashCode());
			Transaction.Commit();
			_log.Debug("Committed transaction: {0}...", Transaction.GetHashCode());
		}

		#region IDisposable
		private bool _disposed = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (_disposed) return;

			if (disposing)
			{
				if (__connection.IsValueCreated)
				{
					// dispose managed state (managed objects implemeting IDisposable).
					if (Transaction != null)
					{
						_log.Debug("Disposing transaction: {0}...", Transaction.GetHashCode());
						Transaction.Dispose();
						_log.Debug("Disposed transaction: {0}...", Transaction.GetHashCode());
						//_transaction = null;
					}

					if (Connection != null)
					{
						_log.Debug("Disposing connection: {0}...", Connection.GetHashCode());
						Connection.Dispose();
						_log.Debug("Disposed connection: {0}...", Connection.GetHashCode());
						//_connection = null;
					}
				}

				__connection = null;

				OnDispose();
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

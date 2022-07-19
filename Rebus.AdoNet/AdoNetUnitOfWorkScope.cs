using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;

using Rebus.Logging;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	public class AdoNetUnitOfWorkScope : IDisposable
	{
		private static ILog _log;
		private bool _completed = false;
		private readonly IAdoNetUnitOfWork _unitOfWork;

		/// <summary>
		/// Gets the SQL dialect of the current connection.
		/// </summary>
		public SqlDialect Dialect { get; private set; }

		/// <summary>
		/// Occurs when [on dispose].
		/// </summary>
		public event Action OnDispose = delegate { };

		/// <summary>
		/// Gets the current open connection to the database
		/// </summary>
		public IDbConnection Connection { get; private set; }


		static AdoNetUnitOfWorkScope()
		{
			RebusLoggerFactory.Changed += f => _log = f.GetCurrentClassLogger();
		}

		public AdoNetUnitOfWorkScope(IAdoNetUnitOfWork unitOfWork, SqlDialect dialect, IDbConnection connection)
		{
			if (unitOfWork == null) throw new ArgumentNullException(nameof(unitOfWork));

			_unitOfWork = unitOfWork;
			Dialect = dialect;
			Connection = connection;

			_log.Debug("Created new instance: {0} (UnitOfWork: {1})", GetHashCode(), unitOfWork.GetHashCode());
		}

		private void EnsureNotDisposed()
		{
			if (_disposed)
			{
				throw new ObjectDisposedException(nameof(AdoNetUnitOfWorkScope));
			}
		}

		public void Complete()
		{
			EnsureNotDisposed();
			_completed = true;
		}

		/// <summary>
		/// Queries sys.Tables in the current DB
		/// </summary>
		public IEnumerable<string> GetTableNames()
		{
			EnsureNotDisposed();
			return Dialect.GetTableNames(Connection);
		}

		#region IDisposable
		private bool _disposed = false; // To detect redundant calls

		protected virtual void Dispose(bool disposing)
		{
			if (_disposed) return;

			if (disposing)
			{
				// dispose managed state (managed objects implemeting IDisposable).
				_log.Debug("Disposing instance: {0} (Completed: {1})", GetHashCode(), _completed);

				if (!_completed)
				{
					_unitOfWork.Abort();
				}

				OnDispose();

				_log.Debug("Disposed instance: {0}...", GetHashCode());
			}

			// XXX: Never throw exceptions from this point forward, log them instead.
			//		Throwing exceptions from a finalizer can crash the app.

			// free unmanaged resources (unmanaged objects), if any, and set large fields to null.
			// Then enable finalizer override found below.

			_disposed = true;
		}

#if false //< override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
		~AdoNetUnitOfWorkScope() {
			// Do not change this code. Put cleanup code in Dispose(bool disposing) above.
			Dispose(false); //< disposing == false, cause we are finalizing here, diposing has already been performed.
		}
#endif

		// Code added to correctly implement the disposable pattern.
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

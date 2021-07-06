using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;

namespace Rebus.AdoNet
{
	public class DisposableTracker
	{
		private static ILog _log = LogManager.GetLogger(typeof(DisposableTracker));
		private readonly List<IDisposable> _disposables = new List<IDisposable>();

		public void TrackDisposable<T>(T disposableToTrack) where T : IDisposable
		{
			lock (_disposables)
			{
				_disposables.Add(disposableToTrack);
			}
		}

		/// <summary>
		/// Disposes all the disposables and empties the list
		/// </summary>
		public void DisposeTheDisposables()
		{
			lock (_disposables)
			{
				var disposables = _disposables.ToList();
				_disposables.Clear();

				foreach (var disposable in disposables)
				{
					try
					{
						_log.InfoFormat("Disposing {0}", disposable);
						disposable.Dispose();
					}
					catch (Exception e)
					{
						_log.ErrorFormat("An error occurred while disposing {0}", e, disposable);
						Console.Error.WriteLine("An error occurred while disposing {0}: {1}", disposable, e);
					}
				}
			}
		}

		public IEnumerable<IDisposable> GetTrackedDisposables()
		{
			lock (_disposables)
			{
				return _disposables.ToList();
			}
		}
	}
}

using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;

namespace Rebus.AdoNet
{
	internal static class DisposableTracker
	{
		private static readonly List<IDisposable> TrackedDisposables = new List<IDisposable>();
		private static ILog _log = LogManager.GetLogger(typeof(DisposableTracker));

		public static void TrackDisposable<T>(T disposableToTrack) where T : IDisposable
		{
			TrackedDisposables.Add(disposableToTrack);
		}

		/// <summary>
		/// Disposes all the disposables and empties the list
		/// </summary>
		public static void DisposeTheDisposables()
		{
			var disposables = TrackedDisposables.ToList();
			TrackedDisposables.Clear();

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

		public static IEnumerable<IDisposable> GetTrackedDisposables()
		{
			return TrackedDisposables.ToList();
		}
	}
}

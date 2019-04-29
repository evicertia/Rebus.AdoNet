extern alias sqlitewin;
extern alias sqlitemac;

using System;
using System.Collections.Generic;
using System.Linq;
using Common.Logging;
using Common.Logging.Simple;

using NUnit.Framework;

namespace Rebus.AdoNet
{
	[SetUpFixture]
	public class AssemblyFixture
	{
		private ILog _log = null;
		private static DisposableTracker _disposables = new DisposableTracker();

		[OneTimeSetUp]
		public void AssemblySetup()
		{
			LogManager.Adapter = new ConsoleOutLoggerFactoryAdapter(LogLevel.All, true, true, true, null);
			_log = LogManager.GetLogger<AssemblyFixture>();

			// Ensure runtime dependency on ado.net providers...
			_log.InfoFormat("Using SQLite Ado.Net (win) provider version: {0}", typeof(sqlitewin::System.Data.SQLite.SQLiteFactory).AssemblyQualifiedName);
			_log.InfoFormat("Using SQLite Ado.Net (mac) provider version: {0}", typeof(sqlitemac::System.Data.SQLite.SQLiteFactory).AssemblyQualifiedName);
			_log.InfoFormat("Using PostgreSql provider version: {0}", typeof(Npgsql.NpgsqlFactory).AssemblyQualifiedName);
		}

		[OneTimeTearDown]
		public void AssemblyTearDown()
		{
			_disposables.DisposeTheDisposables();
		}

		public static T TrackDisposable<T>(T disposable) where T : IDisposable
		{
			_disposables.TrackDisposable(disposable);
			return disposable;
		}
	}
}

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

		[OneTimeSetUp]
		public void AssemblySetup()
		{
			LogManager.Adapter = new ConsoleOutLoggerFactoryAdapter(LogLevel.All, true, true, true, null);
			_log = LogManager.GetLogger<AssemblyFixture>();

			// Ensure runtime dependency on csharp-sqlite assembly.
			_log.InfoFormat("Using csharp-sqlite provider version: {0}", typeof(Community.CsharpSqlite.SQLiteClient.SqliteClientFactory).AssemblyQualifiedName);
		}
	}
}

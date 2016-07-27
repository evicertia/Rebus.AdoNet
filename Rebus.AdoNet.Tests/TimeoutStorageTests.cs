using System;
using System.IO;

using Common.Logging;
using NUnit.Framework;

namespace Rebus.AdoNet
{
	[TestFixture]
	public class TimeoutStorageTests : DatabaseFixtureBase
	{
		private static readonly ILog _Log = LogManager.GetLogger<SagaPersisterTests>();
		private const string PROVIDER_NAME = "csharp-sqlite";
		private const string CONNECTION_STRING = @"Data Source=file://{0};Version=3;New=True;";
		private const string TIMEOUTS_TABLE = "timeouts";

		private AdoNetTimeoutStorage _storage;

		public TimeoutStorageTests()
			: base(GetConnectionString(), PROVIDER_NAME)
		{
		}

		private static string GetConnectionString()
		{
			var dbfile = AssemblyFixture.TrackDisposable(new TempFile());
			File.Delete(dbfile.Path);
			_Log.DebugFormat("Using temporal file: {0}", dbfile.Path);
			return string.Format(CONNECTION_STRING, dbfile.Path);
		}

		[Test]
		public void CanCreateStorageTableAutomatically()
		{
			_storage.EnsureTableIsCreated();

			var tableNames = GetTableNames();
			Assert.That(tableNames, Contains.Item(TIMEOUTS_TABLE));
		}

		[Test]
		public void DoesntDoAnythingIfTheTableAlreadyExists()
		{
			ExecuteCommand(@"CREATE TABLE """ + TIMEOUTS_TABLE + @""" (""id"" INT NOT NULL)");

			_storage.EnsureTableIsCreated();
			_storage.EnsureTableIsCreated();
			_storage.EnsureTableIsCreated();
		}

		protected override void OnSetUp()
		{
			DropTable(TIMEOUTS_TABLE);
	
			_storage = new AdoNetTimeoutStorage(ConnectionString, ProviderName, TIMEOUTS_TABLE);
		}
	}
}
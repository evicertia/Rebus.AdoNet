using System;
using System.Data;
using System.Linq;

namespace Rebus.AdoNet.Dialects
{
	public class YugabyteDbDialect : PostgreSqlDialect
	{
		protected override Version MinimumDatabaseVersion => new Version("11.0");
		public override bool SupportsSelectForWithSkipLocked => false;
		public override bool SupportsSelectForWithNoWait => false;
		public override bool SupportsTryAdvisoryLockFunction => false;

		public override string GetDatabaseVersion(IDbConnection connection)
		{
			var result = connection.ExecuteScalar("SHOW server_version;");
			var versionString = Convert.ToString(result);
			return versionString.Split('-').First();
		}
	}
}
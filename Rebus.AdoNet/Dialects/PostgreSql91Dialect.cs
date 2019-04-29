using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql91Dialect : PostgreSql82Dialect
	{
		private Version MinimumDatabaseVersion => new Version("9.1");

		public override bool SupportsTryAdvisoryXactLockFunction => true;
		public override ushort Priority => 91;

		public override bool SupportsThisDialect(IDbConnection connection)
		{
			try
			{
				var versionString = (string)connection.ExecuteScalar(@"SELECT VERSION();");
				var databaseVersion = new Version(this.GetDatabaseVersion(connection));
				return versionString.StartsWith("PostgreSQL ") && databaseVersion >= MinimumDatabaseVersion;
			}
			catch
			{
				return false;
			}
		}

		public override string FormatTryAdvisoryXactLock(IEnumerable<object> args)
		{
			var @params = "";

			if (args.Count() > 1)
			{
				@params = string.Join(",", args);
			}
			else
			{
				@params = Convert.ToString(args.First());
			}

			return $"pg_try_advisory_xact_lock({@params})";
		}
	}
}

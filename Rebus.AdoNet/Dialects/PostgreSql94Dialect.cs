using System;
using System.Data;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql94Dialect : PostgreSql91Dialect
	{
		protected override Version MinimumDatabaseVersion => new Version("9.4");
		public override ushort Priority => 94;

		public override bool SupportsTableExpressions => true;

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
	}
}

using System;
using System.Data;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql82Dialect : PostgreSqlDialect
	{
		private Version MinimumDatabaseVersion => new Version("8.2");
		public override ushort Priority => 82;

		public override bool SupportsReturningClause => true;

		public override bool SupportsThisDialect(IDbConnection connection)
		{
			try
			{
				var versionString = (string)connection.ExecuteScalar(@"SELECT VERSION();");
				var databaseVersion = new Version(this.GetDatabaseVersion(connection));
				return versionString.StartsWith("PostgreSQL ", StringComparison.Ordinal) && databaseVersion >= MinimumDatabaseVersion;
			}
			catch
			{
				return false;
			}
		}
	}
}

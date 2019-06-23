using System;
using System.Data;
using System.Data.Common;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql82Dialect : PostgreSqlDialect
	{
		protected override Version MinimumDatabaseVersion => new Version("8.2");
		public override ushort Priority => 82;

		public override bool SupportsReturningClause => true;
		public override bool SupportsSelectForWithNoWait => true;
		public override string SelectForNoWaitClause => "NOWAIT";

		public override bool IsSelectForNoWaitLockingException(DbException ex)
		{
			if (ex != null && ex.GetType().Name == "NpgsqlException")
			{
				var psqlex = new PostgreSqlExceptionAdapter(ex);
				return psqlex.Code == "55P03";
			}

			return false;
		}

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

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Data.Common;

namespace Rebus.AdoNet.Dialects
{
	public class YugabyteDbDialect : PostgreSqlDialect
	{
		private static readonly IEnumerable<string> _postgresExceptionNames = new[] { "NpgsqlException", "PostgresException" };

		// TODO: Provide a better (finer grained) version matching logic for Yugabyte.
		protected override Version MinimumDatabaseVersion => new Version("11.0");
		// XXX: https://github.com/yugabyte/yugabyte-db/issues/2742
		public override bool SupportsSelectForWithSkipLocked => true;
		// XXX: https://github.com/yugabyte/yugabyte-db/issues/2742
		public override bool SupportsSelectForWithNoWait => true;
		public override bool SupportsTryAdvisoryLockFunction => false;
		public override bool SupportsReturningClause => true;
		public override bool SupportsTableExpressions => true;
		public override bool SupportsOnConflictClause => true;
		public override string SelectForUpdateClause => "FOR UPDATE";
		public override string SelectForSkipLockedClause => "SKIP LOCKED";
		public override string SelectForNoWaitClause => "NOWAIT";
		public override bool SupportsGinIndexes => true;
		public override bool SupportsMultiColumnGinIndexes => false;
		public override bool SupportsJsonColumns => true;
		public override string JsonColumnGinPathIndexOpclass => "jsonb_path_ops";

		public YugabyteDbDialect()
		{
			RegisterColumnType(DbType.Object, "jsonb");
		}
		
		public override string GetDatabaseVersion(IDbConnection connection)
		{
			var result = connection.ExecuteScalar("SHOW server_version;");
			var versionString = Convert.ToString(result);
			return versionString.Split('-').First();
		}
		
		public override bool IsSelectForNoWaitLockingException(DbException ex)
		{
			if (ex != null && _postgresExceptionNames.Contains(ex.GetType().Name))
			{
				var psqlex = new PostgreSqlExceptionAdapter(ex);
				return psqlex.Code == "55P03";
			}

			return false;
		}
		
		public override bool IsDuplicateKeyException(DbException ex)
		{
			if (ex != null && _postgresExceptionNames.Contains(ex.GetType().Name))
			{
				var psqlex = new PostgreSqlExceptionAdapter(ex);
				return psqlex.Code == "23505";
			}

			return false;
		}
	}
}
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql82Dialect : PostgreSqlDialect
	{
		private static readonly IEnumerable<string> _postgresExceptionNames = new[] { "NpgsqlException", "PostgresException" };

		protected override Version MinimumDatabaseVersion => new Version("8.2");
		public override ushort Priority => 82;

		public override bool SupportsReturningClause => true;
		public override bool SupportsSelectForWithNoWait => true;
		public override string SelectForNoWaitClause => "NOWAIT";

		public override bool IsSelectForNoWaitLockingException(DbException ex)
		{
			if (ex != null && _postgresExceptionNames.Contains(ex.GetType().Name))
			{
				var psqlex = new PostgreSqlExceptionAdapter(ex);
				return psqlex.Code == "55P03";
			}

			return false;
		}
	}
}

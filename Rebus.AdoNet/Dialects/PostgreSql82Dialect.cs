using System;
using System.Data;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql82Dialect : PostgreSqlDialect
	{
		protected override Version MinimumDatabaseVersion => new Version("8.2");

		public override ushort Priority => 82;
		public override bool SupportsReturningClause => true;
	}
}

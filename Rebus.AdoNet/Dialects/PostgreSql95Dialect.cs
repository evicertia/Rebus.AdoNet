using System;
using System.Data;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSql95Dialect : PostgreSql94Dialect
	{
		protected override Version MinimumDatabaseVersion => new Version("9.5");

		public override ushort Priority => 95;
		public override bool SupportsOnConflictClause => true;
		public override bool SupportsSkipLockedFunction => true;
		public override string ParameterSkipLocked => "SKIP LOCKED";

	}
}

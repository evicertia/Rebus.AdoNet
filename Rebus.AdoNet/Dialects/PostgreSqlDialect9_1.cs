using System;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSqlDialect9_1 : PostgreSqlDialect
	{
		public override bool SupportsTryAdvisoryXactLockFunction => true;

		// Version 9.1
		public override Version DatabaseVersion => new Version("9.1");
		public override string Sql => @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
										FROM ""{0}""
										WHERE ""time_to_return"" <= @current_time AND pg_try_advisory_xact_lock(""id"")
										ORDER BY ""time_to_return"" ASC";

		public PostgreSqlDialect9_1() : base()
		{
		}
	}
}

using System;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSqlDialect9_5 : PostgreSqlDialect
	{
		public override bool SupportsSkipLockedFunction => true;
		public override string ParameterSkipLocked => "SKIP  LOCKED";

		// Version 9.5 or 9.5+
		public override Version DatabaseVersion => new Version("9.5");
		public override string Sql =>  @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
								FROM ""{0}""
								WHERE ""time_to_return"" <= @current_time
								ORDER BY ""time_to_return"" ASC
								FOR UPDATE SKIP LOCKED";

		public PostgreSqlDialect9_5() : base()
		{
		}
	}
}

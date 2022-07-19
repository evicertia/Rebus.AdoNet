using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Rebus.AdoNet.Dialects {
	public class PostgreSql92Dialect : PostgreSql91Dialect
	{
		protected override Version MinimumDatabaseVersion => new Version("9.2");

		public override ushort Priority => 92;

		public override bool SupportsJsonColumns => true;
		public override string JsonColumnGinPathIndexOpclass => "jsonb_path_ops";

		public PostgreSql92Dialect()
		{
			RegisterColumnType(DbType.Object, "jsonb");
		}

		public override string Cast(string expression, DbType type) => $"({expression})::{GetColumnType(type)}";
	}
}
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet.Dialects
{
	public class PostgreSqlDialect : SqlDialect
	{
		public PostgreSqlDialect()
		{

			RegisterColumnType(DbType.AnsiStringFixedLength, "char(255)");
			RegisterColumnType(DbType.AnsiStringFixedLength, 8000, "char($l)");
			RegisterColumnType(DbType.AnsiString, "varchar(255)");
			RegisterColumnType(DbType.AnsiString, 8000, "varchar($l)");
			RegisterColumnType(DbType.AnsiString, 2147483647, "text");
			RegisterColumnType(DbType.Binary, "bytea");
			RegisterColumnType(DbType.Binary, 2147483647, "bytea");
			RegisterColumnType(DbType.Boolean, "boolean");
			RegisterColumnType(DbType.Byte, "int2");
			RegisterColumnType(DbType.Currency, "decimal(16,4)");
			RegisterColumnType(DbType.Date, "date");
			RegisterColumnType(DbType.DateTime, "timestamp");
			RegisterColumnType(DbType.DateTimeOffset, "timestamp with time zone");
			RegisterColumnType(DbType.Decimal, "decimal(19,5)");
			RegisterColumnType(DbType.Decimal, 19, "decimal(18, $l)");
			RegisterColumnType(DbType.Decimal, 19, "decimal($p, $s)");
			RegisterColumnType(DbType.Double, "float8");
			RegisterColumnType(DbType.Int16, "int2");
			RegisterColumnType(DbType.Int32, "int4");
			RegisterColumnType(DbType.Int64, "int8");
			RegisterColumnType(DbType.Single, "float4");
			RegisterColumnType(DbType.StringFixedLength, "char(255)");
			RegisterColumnType(DbType.StringFixedLength, 4000, "char($l)");
			RegisterColumnType(DbType.String, "varchar(255)");
			RegisterColumnType(DbType.String, 4000, "varchar($l)");
			RegisterColumnType(DbType.String, 1073741823, "text");
			RegisterColumnType(DbType.Time, "time");
			RegisterColumnType(DbType.Guid, "uuid");

		}

		#region Overrides
		public override bool SupportsSelectForUpdate => true;
		public override bool SupportsTryAdvisoryLockFunction => true;
		public override string ParameterSelectForUpdate => "FOR UPDATE";

		public virtual string ParameterSkipLocked => string.Empty;

		// Version less than 9.1
		public virtual Version DatabaseVersion => new Version("8.4");
		public virtual string Sql =>  @"SELECT ""id"", ""time_to_return"", ""correlation_id"", ""saga_id"", ""reply_to"", ""custom_data""
										FROM ""{0}""
										WHERE ""time_to_return"" <= @current_time AND pg_try_advisory_lock(""id"")
										ORDER BY ""time_to_return"" ASC";

		public override bool SupportsThisDialect(IDbConnection connection)
		{
			try
			{
				var versionString = (string)connection.ExecuteScalar(@"SELECT VERSION();");
				return versionString.StartsWith("PostgreSQL ");
			}
			catch
			{
				return false;
			}
		}
		#endregion

		#region GetColumnType

		private static string GetIdentityTypeFor(DbType type)
		{
			switch (type)
			{
				case DbType.Int16: return "smallserial";
				case DbType.Int32: return "serial";
				case DbType.Int64: return "bigserial";
				default: throw new ArgumentOutOfRangeException($"Invalid identity column type: {type}");
			}
		}

		public override string GetColumnType(DbType type, uint length, uint precision, uint scale, bool identity, bool primary)
		{
			var result = identity ? GetIdentityTypeFor(type)
				: base.GetColumnType(type, length, precision, scale, identity, primary);

			return result;
		}

		public override string GetSql(Version version)
		{
			if(version >= new Dialects.PostgreSqlDialect9_5().DatabaseVersion)
			{
				return new Dialects.PostgreSqlDialect9_5().Sql;
			}

			if (version >= new Dialects.PostgreSqlDialect9_1().DatabaseVersion)
			{
				return new Dialects.PostgreSqlDialect9_1().Sql;
			}

			return new Dialects.PostgreSqlDialect().Sql;
		}
		#endregion
	}
}

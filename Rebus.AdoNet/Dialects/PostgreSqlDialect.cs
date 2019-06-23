using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Rebus.AdoNet.Schema;

namespace Rebus.AdoNet.Dialects
{
	/// <summary>
	/// Postgre sql dialect, assuming v8.0 as bare minimum.
	/// </summary>
	public class PostgreSqlDialect : SqlDialect
	{
		protected virtual Version MinimumDatabaseVersion => new Version("8.0");

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

		public override string GetDatabaseVersion(IDbConnection connection)
		{
			var result = connection.ExecuteScalar("SHOW server_version;");
			var versionString = Convert.ToString(result);
			return versionString.Split(' ').First();
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
		#endregion

		#region GetColumnType

		public override string GetIdentityTypeFor(DbType type)
		{
			switch (type)
			{
				case DbType.Int16: return "smallserial";
				case DbType.Int32: return "serial";
				case DbType.Int64: return "bigserial";
				default: throw new ArgumentOutOfRangeException($"Invalid identity column type: {type}");
			}
		}

		public override string FormatTryAdvisoryLock(IEnumerable<object> args)
		{
			var @params = "";

			if (args.Count() > 1)
			{
				@params = string.Join(",", args);
			}
			else
			{
				@params = Convert.ToString(args.First());
			}

			return $"pg_try_advisory_lock({@params})";
		}
		#endregion
	}
}

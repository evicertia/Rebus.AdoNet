using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Rebus.AdoNet.Dialects
{
	public class SqliteDialect : SqlDialect
	{
		public override bool SeparatePrimaryKeyDeclaration => false;

		public SqliteDialect()
		{
			RegisterColumnType(DbType.Binary, "BLOB");
			RegisterColumnType(DbType.Byte, "TINYINT");
			RegisterColumnType(DbType.Int16, "SMALLINT");
			RegisterColumnType(DbType.Int32, "INT");
			RegisterColumnType(DbType.Int64, "BIGINT");
			RegisterColumnType(DbType.SByte, "INTEGER");
			RegisterColumnType(DbType.UInt16, "INTEGER");
			RegisterColumnType(DbType.UInt32, "INTEGER");
			RegisterColumnType(DbType.UInt64, "INTEGER");
			RegisterColumnType(DbType.Currency, "NUMERIC");
			RegisterColumnType(DbType.Decimal, "NUMERIC");
			RegisterColumnType(DbType.Double, "DOUBLE");
			RegisterColumnType(DbType.Single, "DOUBLE");
			RegisterColumnType(DbType.VarNumeric, "NUMERIC");
			RegisterColumnType(DbType.AnsiString, "TEXT");
			RegisterColumnType(DbType.String, "TEXT");
			RegisterColumnType(DbType.AnsiStringFixedLength, "TEXT");
			RegisterColumnType(DbType.StringFixedLength, "TEXT");

			RegisterColumnType(DbType.Date, "DATE");
			RegisterColumnType(DbType.DateTime, "DATETIME");
			RegisterColumnType(DbType.DateTimeOffset, "DATETIME");
			RegisterColumnType(DbType.Time, "TIME");
			RegisterColumnType(DbType.Boolean, "BOOL");
			RegisterColumnType(DbType.Guid, "UNIQUEIDENTIFIER");
		}

		public override string GetDatabaseVersion(IDbConnection connection)
		{
			return (string)connection.ExecuteScalar(@"SELECT sqlite_version();");
		}

		public override bool SupportsThisDialect(IDbConnection connection)
		{
			try
			{
				GetDatabaseVersion(connection);
			}
			catch (Exception ex)
			{
				Console.WriteLine("Failed: {0}", ex);
				return false;
			}

			return true;
		}

		public override string GetColumnType(DbType type, uint length, uint precision, uint scale, bool identity, bool primary)
		{
			return identity ?
				"INTEGER PRIMARY KEY AUTOINCREMENT" //< This is all you can get from sqlite's identity/auto-increment support.
				: base.GetColumnType(type, length, precision, scale, identity, primary);
		}
	}
}

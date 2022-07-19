using System.Data.Common;
using System.Collections.Generic;

namespace System.Data
{
	internal static class IDbConnectionExtensions
	{
		/// <summary>
		/// Fetch the column names with their data types..
		///		where the Tuple.Item1 is the column name and the Tuple.Item2 is the data type.
		/// </summary>
		public static IEnumerable<Tuple<string, string>> GetColumnSchemaFor(this IDbConnection @this, string tableName)
		{
			if (@this == null)
				throw new ArgumentNullException(nameof(@this));

			if (string.IsNullOrWhiteSpace(tableName))
				throw new ArgumentNullException(nameof(tableName));

			// XXX: In order, to retrieve the schema information we can specify
			//		the catalog (0), schema (1), table name (2) and column name (3).
			var restrictions = new string[4];
			restrictions[2] = tableName;

			var data = new List<Tuple<string, string>>();
			var schemas = (@this as DbConnection).GetSchema("Columns", restrictions);

			foreach (DataRow row in schemas.Rows)
			{
				var name = row["COLUMN_NAME"] as string;
				var type = row["DATA_TYPE"] as string;
				data.Add(Tuple.Create(name, type));
			}

			return data.ToArray();
		}

		/// <summary>
		/// Retrieve table's indexes for a specific table.
		/// </summary>
		public static IEnumerable<string> GetIndexesFor(this IDbConnection @this, string tableName)
		{
			if (@this == null)
				throw new ArgumentNullException(nameof(@this));

			if (string.IsNullOrWhiteSpace(tableName))
				throw new ArgumentNullException(nameof(tableName));

			// XXX: In order, to retrieve the schema information we can specify
			//		the catalog (0), schema (1), table name (2) and column name (3).
			var restrictions = new string[4];
			restrictions[2] = tableName;

			var data = new List<string>();
			var schemas = (@this as DbConnection).GetSchema("Indexes", restrictions);

			foreach (DataRow row in schemas.Rows)
				data.Add(row["INDEX_NAME"] as string);

			return data.ToArray();
		}
	}
}

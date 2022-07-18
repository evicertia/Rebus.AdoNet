using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Rebus.AdoNet.Dialects
{
	/// <summary>
	/// This class maps a DbType to names.
	/// </summary>
	/// <remarks>
	/// Associations may be marked with a capacity. Calling the <c>Get()</c>
	/// method with a type and actual size n will return the associated
	/// name with smallest capacity >= n, if available and an unmarked
	/// default type otherwise.
	/// Eg, setting
	/// <code>
	///     Names.Put(DbType,           "TEXT" );
	///     Names.Put(DbType,   255,    "VARCHAR($l)" );
	///     Names.Put(DbType,   65534,  "LONGVARCHAR($l)" );
	/// </code>
	/// will give you back the following:
	/// <code>
	///     Names.Get(DbType)           // --> "TEXT" (default)
	///     Names.Get(DbType,100)       // --> "VARCHAR(100)" (100 is in [0:255])
	///     Names.Get(DbType,1000)  // --> "LONGVARCHAR(1000)" (100 is in [256:65534])
	///     Names.Get(DbType,100000)    // --> "TEXT" (default)
	/// </code>
	/// On the other hand, simply putting
	/// <code>
	///     Names.Put(DbType, "VARCHAR($l)" );
	/// </code>
	/// would result in
	/// <code>
	///     Names.Get(DbType)           // --> "VARCHAR($l)" (will cause trouble)
	///     Names.Get(DbType,100)       // --> "VARCHAR(100)" 
	///     Names.Get(DbType,1000)  // --> "VARCHAR(1000)"
	///     Names.Get(DbType,10000) // --> "VARCHAR(10000)"
	/// </code>
	/// </remarks>
	public class TypeNames
	{
		public const string LengthPlaceHolder = "$l";
		public const string PrecisionPlaceHolder = "$p";
		public const string ScalePlaceHolder = "$s";

		private readonly Dictionary<DbType, SortedList<uint, string>> weighted = new Dictionary<DbType, SortedList<uint, string>>();
		private readonly Dictionary<DbType, string> defaults = new Dictionary<DbType, string>();

		/// <summary>
		/// 
		/// </summary>
		/// <param name="template"></param>
		/// <param name="placeholder"></param>
		/// <param name="replacement"></param>
		/// <returns></returns>
		private static string ReplaceOnce(string template, string placeholder, string replacement)
		{
			int loc = template.IndexOf(placeholder, StringComparison.Ordinal);
			if (loc < 0)
			{
				return template;
			}
			else
			{
				return new StringBuilder(template.Substring(0, loc))
					.Append(replacement)
					.Append(template.Substring(loc + placeholder.Length))
					.ToString();
			}
		}

		/// <summary>
		/// Replaces the specified type.
		/// </summary>
		/// <param name="type">The type.</param>
		/// <param name="size">The size.</param>
		/// <param name="precision">The precision.</param>
		/// <param name="scale">The scale.</param>
		/// <returns></returns>
		private static string Replace(string type, uint size, uint precision, uint scale)
		{
			type = ReplaceOnce(type, LengthPlaceHolder, size.ToString());
			type = ReplaceOnce(type, ScalePlaceHolder, scale.ToString());
			return ReplaceOnce(type, PrecisionPlaceHolder, precision.ToString());
		}

		/// <summary>
		/// Get default type name for specified type
		/// </summary>
		/// <param name="typecode">the type key</param>
		/// <returns>the default type name associated with the specified key</returns>
		public string Get(DbType typecode)
		{
			string result;
			if (!defaults.TryGetValue(typecode, out result))
			{
				throw new ArgumentException("Dialect does not support DbType." + typecode, "typecode");
			}
			return result;
		}

		/// <summary>
		/// Get the type name specified type and size
		/// </summary>
		/// <param name="typecode">the type key</param>
		/// <param name="size">the SQL length </param>
		/// <param name="scale">the SQL scale </param>
		/// <param name="precision">the SQL precision </param>
		/// <returns>
		/// The associated name with smallest capacity >= size if available and the
		/// default type name otherwise
		/// </returns>
		public string Get(DbType typecode, uint size, uint precision, uint scale)
		{
			SortedList<uint, string> map;
			weighted.TryGetValue(typecode, out map);
			if (map != null && map.Count > 0)
			{
				foreach (KeyValuePair<uint, string> entry in map)
				{
					if (size <= entry.Key)
					{
						return Replace(entry.Value, size, precision, scale);
					}
				}
			}
			//Could not find a specific type for the size, using the default
			return Replace(Get(typecode), size, precision, scale);
		}

		/// <summary>
		/// For types with a simple length, this method returns the definition
		/// for the longest registered type.
		/// </summary>
		/// <param name="typecode"></param>
		/// <returns></returns>
		public string GetLongest(DbType typecode)
		{
			SortedList<uint, string> map;
			weighted.TryGetValue(typecode, out map);

			if (map != null && map.Count > 0)
				return Replace(map.Values[map.Count - 1], map.Keys[map.Count - 1], 0, 0);

			return Get(typecode);
		}

		/// <summary>
		/// Set a type name for specified type key and capacity
		/// </summary>
		/// <param name="typecode">the type key</param>
		/// <param name="capacity">the (maximum) type size/length</param>
		/// <param name="value">The associated name</param>
		public void Put(DbType typecode, uint capacity, string value)
		{
			SortedList<uint, string> map;
			if (!weighted.TryGetValue(typecode, out map))
			{
				// add new ordered map
				weighted[typecode] = map = new SortedList<uint, string>();
			}
			map[capacity] = value;
		}

		/// <summary>
		/// 
		/// </summary>
		/// <param name="typecode"></param>
		/// <param name="value"></param>
		public void Put(DbType typecode, string value)
		{
			defaults[typecode] = value;
		}
	}
}
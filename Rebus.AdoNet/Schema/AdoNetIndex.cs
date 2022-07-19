using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Rebus.AdoNet.Schema
{
	public class AdoNetIndex
	{
		public enum SortOrder
		{
			Unspecified = -1,
			Ascending = 0,
			Descending = 1
		}

		public enum Kinds
		{
			Default = 0,
			BTree = 1,
			GIN = 2
		}

		public string Name { get; set; }
		public string[] Columns { get; set; }
		public SortOrder Order { get; set; }
		public Kinds Kind { get; set; }
	}
}

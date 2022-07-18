using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Rebus.AdoNet.Schema
{
	public class AdoNetTable
	{
		public string Name { get; set; }
		public IEnumerable<AdoNetColumn> Columns { get; set; }
		public string[] PrimaryKey { get; set; }
		public IEnumerable<AdoNetIndex> Indexes { get; set; }

		public bool HasCompositePrimaryKey => PrimaryKey?.Count() > 1;
	}
}

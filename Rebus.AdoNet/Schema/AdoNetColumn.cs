using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Rebus.AdoNet.Schema
{
	public class AdoNetColumn
	{
		public string Name { get; set; }
		public DbType DbType { get; set; }
		public uint Length { get; set; }
		public uint Precision { get; set; }
		public uint Scale { get; set; }
		public bool Nullable { get; set; }
		public bool Identity { get; set; }
		public bool Array { get; set; }
		public object Default { get; set; }
	}
}

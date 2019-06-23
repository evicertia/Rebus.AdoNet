using System;
using System.Data.Common;

namespace Rebus.AdoNet
{
	public class AdoNetSagaLockedException : Exception
	{
		public AdoNetSagaLockedException(DbException inner)
			: base("Error while trying lock saga data: already locked.", inner)
		{
		}
	}
}

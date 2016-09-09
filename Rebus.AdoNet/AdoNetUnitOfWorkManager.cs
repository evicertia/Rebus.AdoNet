using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Rebus;
using Rebus.Bus;
using Rebus.AdoNet.Dialects;

namespace Rebus.AdoNet
{
	public class AdoNetUnitOfWorkManager : IUnitOfWorkManager
	{
		private const string CONTEXT_ITEM_KEY = nameof(AdoNetUnitOfWork);
		private readonly AdoNetConnectionFactory _factory;

		public AdoNetUnitOfWorkManager(AdoNetConnectionFactory factory)
		{
			Guard.NotNull(() => factory, factory);

			_factory = factory;
		}

		internal static AdoNetUnitOfWork GetCurrent()
		{
			object result = null;
			var context = MessageContext.HasCurrent ? MessageContext.GetCurrent() : null;

			if ((bool)context?.Items.TryGetValue(CONTEXT_ITEM_KEY, out result))
			{
				return (AdoNetUnitOfWork)result;
			}

			return null;
		}

		public IUnitOfWork Create()
		{
			var context = MessageContext.GetCurrent();
			var result = new AdoNetUnitOfWork(_factory, false);
			context.Items.Add(CONTEXT_ITEM_KEY, result);

			return result;
		}
	}
}

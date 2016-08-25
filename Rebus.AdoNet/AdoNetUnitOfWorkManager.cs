using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Rebus;
using Rebus.Bus;

namespace Rebus.AdoNet
{
	public class AdoNetUnitOfWorkManager : IUnitOfWorkManager
	{
		private const string CONTEXT_ITEM_KEY = nameof(AdoNetUnitOfWork);

		internal static AdoNetUnitOfWork GetUnitOfWorkFor(IMessageContext context)
		{
			object result = null;

			if (context.Items.TryGetValue(CONTEXT_ITEM_KEY, out result))
			{
				return (AdoNetUnitOfWork)result;
			}

			return null;
		}

		public IUnitOfWork Create()
		{
			var context = MessageContext.GetCurrent();
			var result = new AdoNetUnitOfWork();
			context.Items.Add(CONTEXT_ITEM_KEY, result);

			return result;
		}
	}
}

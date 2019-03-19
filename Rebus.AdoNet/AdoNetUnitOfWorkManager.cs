﻿using System;
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
		private readonly Func<AdoNetConnectionFactory, IMessageContext, AdoNetUnitOfWork> _unitOfWorkCreator;

		public AdoNetUnitOfWorkManager(AdoNetConnectionFactory factory, Func<AdoNetConnectionFactory, IMessageContext, AdoNetUnitOfWork> unitOfWorkCreator)
		{
			Guard.NotNull(() => factory, factory);
			Guard.NotNull(() => unitOfWorkCreator, unitOfWorkCreator);

			_factory = factory;
			_unitOfWorkCreator = unitOfWorkCreator;
		}

		internal static AdoNetUnitOfWork TryGetCurrent()
		{
			object result = null;
			var context = MessageContext.HasCurrent ? MessageContext.GetCurrent() : null;

			if (context != null)
			{
				if (context.Items.TryGetValue(CONTEXT_ITEM_KEY, out result))
				{
					return (AdoNetUnitOfWork)result;
				}
			}

			return null;
		}

		internal AdoNetUnitOfWorkScope GetScope(bool autonomous = true, bool autocomplete = false)
		{
			var uow = TryGetCurrent();
			if (!autonomous && uow == null)
			{
				throw new InvalidOperationException("An scope was requested, but no UnitOfWork was avaialble?!");
			}

			uow = uow ?? new AdoNetUnitOfWork(_factory, null);

			var result = uow.GetScope();
			if (autocomplete) result.Complete();

			return result;
		}

		public IUnitOfWork Create()
		{
			var context = MessageContext.GetCurrent();
			var result = _unitOfWorkCreator(_factory, context);
			// Remove from context on disposal..
			//result.OnDispose += () => context.Items.Remove(CONTEXT_ITEM_KEY);
			context.Items.Add(CONTEXT_ITEM_KEY, result);

			return result;
		}
	}
}

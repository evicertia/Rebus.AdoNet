using System;
using System.Collections.Generic;
using Rebus.Serialization.Json;
using Newtonsoft.Json.Serialization;

namespace Rebus.AdoNet {
	internal class CustomSerializationBinder : DefaultSerializationBinder {

		internal Func<TypeDescriptor, Type> NameToTypeResolver = null;
		internal Func<Type, TypeDescriptor> TypeToNameResolver = null;

		public override void BindToName(Type serializedType, out string assemblyName, out string typeName)
		{
			if (TypeToNameResolver != null) {
				var result = TypeToNameResolver(serializedType);
				if (result != null) {
					assemblyName = result.AssemblyName;
					typeName = result.TypeName;
					return;
				}
			}

			base.BindToName(serializedType, out assemblyName, out typeName);
		}

		public override Type BindToType(string assemblyName, string typeName)
		{
			if (NameToTypeResolver != null) {
				var desc = new TypeDescriptor(assemblyName, typeName);
				var type = NameToTypeResolver(desc);
				if (type != null) {
					return type;
				}
			}

			return base.BindToType(assemblyName, typeName);
		}
	}
}

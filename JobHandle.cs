using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IndustrialProcessingSystem
{
    public class JobHandle
    {
        public Guid Id { get; set; }
        public Task<int> Result { get; set; }
    }
}

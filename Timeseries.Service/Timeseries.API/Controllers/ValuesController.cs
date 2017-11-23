using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using TSManager;
using Microsoft.AspNetCore.Authorization;
using Newtonsoft.Json;

namespace Timeseries.API.Controllers
{
    [AllowAnonymous]
    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        private readonly TsManager _tsManager = new TsManager();

        [HttpGet]
        [Route("Tags")]
        public async Task<List<string>> Tags()
        {
            return await _tsManager.GetAllTags();
        }

        [HttpGet]
        [Route("")]

        [HttpPost]
        [Route("Ingest")]
        public async void PostIngest([FromBody]object value)
        {
            await _tsManager.ParseJson(value);
        }

        [HttpPost]
        [Route("Datapoints")]
        public async Task<IList<string>> PostDatapoints([FromBody]object query)
        {
            var result = JsonConvert.DeserializeObject<TsQueryModel>(query.ToString());
           return  await _tsManager.GetDataPoints(result);
        }
    }
}

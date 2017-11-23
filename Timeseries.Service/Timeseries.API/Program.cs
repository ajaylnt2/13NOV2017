using System.IO;
using Microsoft.AspNetCore.Hosting;

namespace Timeseries.API
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = new WebHostBuilder()
                .UseKestrel()
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseWebRoot("MyWebAPI")
                .UseIISIntegration() //IIS Support
                .UseStartup<Startup>()
                .UseApplicationInsights()
                .Build();

            host.Run();
                 
        }
    }
}

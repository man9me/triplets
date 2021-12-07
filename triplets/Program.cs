// See https://aka.ms/new-console-template for more information


using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Running;
using Counter = System.Collections.Generic.Dictionary<string, int>;
public class Tests
{
   

    
   
     [Arguments("lor.txt")]
     [Benchmark]
      public async Task tplt(string path)
      {
         Counter result=new Counter();
         int chunkSize=128000; 
         BufferBlock<char[]> queue;
         
         queue = new BufferBlock<char[]>(new DataflowBlockOptions()
         {
            BoundedCapacity = 32
         });
         async Task read(string path)
         {
            using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: chunkSize);
            using var sr = new StreamReader(fs);
        
            var readSize = 1;
            char[] chunk= new char[chunkSize];
            int numRead;
            while ((numRead = await sr.ReadAsync(chunk, 0, chunkSize)) != 0)
            {
               await queue.SendAsync(chunk);
            }
            queue.Complete();
      
         }
         DataflowLinkOptions DataflowLinkOptions = new() { PropagateCompletion = true };
         static bool IsSkip(char ch)
         {
            return !char.IsLetterOrDigit(ch);
         }
         var count = new ActionBlock<char[]>(buffer =>
         {
            
            for (int i = 0; i < chunkSize - 2; i++)
            {
               if (IsSkip(buffer[i]) || IsSkip(buffer[i + 1]) || IsSkip(buffer[i + 2]))
                  continue;
               var key = new string(buffer, i, 3);
               result[key] = result.GetValueOrDefault(key, 0) + 1;
            }
            new ExecutionDataflowBlockOptions()
            {
               MaxDegreeOfParallelism = 8,
               BoundedCapacity = 32
            };
         });
         queue.LinkTo(count, DataflowLinkOptions);
         await read(path);
         await count.Completion;
         
         var list = result.ToList();
         list.Sort((x, y) => x.Value.CompareTo(y.Value));
         var l= list.TakeLast(10).Reverse().ToArray();
            Console.WriteLine("Top {0}",10);
            foreach (var l1 in l)
            {
               Console.WriteLine(l1);
            }
      }
      static async Task Main()
      {
         var timer = new Stopwatch();
         timer.Start();
         var a= new Tests();
         await a.tplt("lor.txt");
         timer.Stop();
         TimeSpan timeTaken = timer.Elapsed;
         Console.WriteLine("It took {0} ms to count",timeTaken.Milliseconds);
         
         var summary = BenchmarkRunner.Run<Tests>();
      }
    
}



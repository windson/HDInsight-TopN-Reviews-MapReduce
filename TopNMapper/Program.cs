using System;
using System.IO;

namespace TopNMapper
{
    class Program
    {
        static void Main(string[] args)
        {
            string line;
            //Hadoop passes data to the mapper on STDIN
            while ((line = Console.ReadLine()) != null)
            {
                var data = line.Split(new string[] { "|||" }, StringSplitOptions.None);
                var productId = data[1];
                var upvotes = Convert.ToDouble(data[3]);
                var total_votes = Convert.ToDouble(data[4]);
                var downvotes = total_votes - upvotes;
                var rating = Convert.ToDouble(data[6]);
                var weighted_review_rating = ((1 + upvotes) * rating + downvotes * (6 - rating)) / (1 + total_votes);
                //write date from every line to the console
                Console.WriteLine($"{productId}\t{weighted_review_rating}");
            }
        }
    }
}

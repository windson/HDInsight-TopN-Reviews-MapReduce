using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace TopNReducer
{
    class Program
    {
        static List<string[]> metadata = new List<string[]>();
        static void Main(string[] args)
        {
            if (!metadata.Any())
                ReadMeta();
            //dictionary of Key: productIds value:sum of all Weighted Avg Rating for product id
            Dictionary<string, double> prodId_WARSum = new Dictionary<string, double>();
            //dictionary of Key: productIds value: # of occurences of this productId in reviews data
            Dictionary<string, double> prodcutId_Occurence = new Dictionary<string, double>();

            int N = 3;
            if (args.Length > 0 && !int.TryParse(args[0], out N))
                N = 3; //if unable to process arg default N to 3


            string data;
            while ((data = Console.ReadLine()) != null)
            {

                // Data from Hadoop is tab-delimited key/value pairs
                var sArr = data.Split('\t');
                string prodId = sArr[0];
                double weighted_review_rating = Convert.ToDouble(sArr[1]);
                if (prodId_WARSum.ContainsKey(prodId))
                {
                    //If contains, sum up the weighted review rating of that product
                    prodId_WARSum[prodId] += weighted_review_rating;
                    //increment the prodId occurence by 1
                    prodcutId_Occurence[prodId]++;
                }
                else
                {
                    //Add the key to the collection
                    prodId_WARSum.Add(prodId, weighted_review_rating);
                    //Add the key prodId anv value 1 if its new addition
                    prodcutId_Occurence.Add(prodId, 1);
                }
            }

            //dictionary of Key: productIds value: Weighted Product Rating
            Dictionary<string, double> prodcutId_WPR = new Dictionary<string, double>();
            foreach (var prodWAR in prodId_WARSum)
            {
                var prodId = prodWAR.Key;
                double weightedProductRating = prodWAR.Value / prodcutId_Occurence[prodId];
                prodcutId_WPR.Add(prodWAR.Key, weightedProductRating);
            }

            Dictionary<string, Dictionary<string, double>> category_prodcutId_WPR = new Dictionary<string, Dictionary<string, double>>();
            foreach (var wpr in prodcutId_WPR)
            {
                var prodMeta = metadata.Where(x => x[0] == wpr.Key).FirstOrDefault();
                if (prodMeta != null)
                {
                    var category = prodMeta[6];

                    if (category_prodcutId_WPR.ContainsKey(category))
                    {
                        category_prodcutId_WPR[category].Add(wpr.Key, wpr.Value);
                    }
                    else
                    {
                        //Add the key to the collection
                        var dict = new Dictionary<string, double>();
                        dict.Add(wpr.Key, wpr.Value);
                        category_prodcutId_WPR.Add(category, dict);
                    }
                }
            }

            foreach (var cat in category_prodcutId_WPR)
            {
                //for every category take N Weighted product Avgs
                var topNForCategory = cat.Value.OrderByDescending(x => x.Value).Take(N);

                foreach (var topN in topNForCategory)
                {
                    var prodMeta = metadata.Where(x => x[0] == topN.Key).FirstOrDefault();
                    if (prodMeta != null)
                    {
                        var category = prodMeta[6];
                        //<<category, Product_Id, Title>, weighted_product_rating>
                        Console.WriteLine($"<<{category}, {topN.Key}, {prodMeta[1]}>,{topN.Value.ToString()}>");
                    }
                }
            }
        }

        private static void ReadMeta()
        {
            using (var fs = File.OpenRead(@"metadata.csv"))
            using (var reader = new StreamReader(fs))
            {

                while (!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var values = line.Split(new string[] { "|||" }, StringSplitOptions.None);
                    metadata.Add(values);
                }
            }
        }
    }
}

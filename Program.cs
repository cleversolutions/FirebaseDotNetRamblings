using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Google.Cloud.Firestore;
using Google.Cloud.Firestore.V1Beta1;
using static Google.Cloud.Firestore.V1Beta1.Target.Types;
using System.Linq;
using Google.Api.Gax;
using System.Collections.Concurrent;
using Google.Protobuf;
using Google.Api.Gax.Grpc;
using static Google.Cloud.Firestore.V1Beta1.StructuredQuery.Types;

namespace FirebaseDotNet
{
    class Program
    {
        static string projectId = "playing-with-firestore";
        //static string projectId = "playing-with-firestore-162c9";
        static string databaseId = "(default)";
        static void Main(string[] args)
        {
            //Create our database connection
            FirestoreDb db = FirestoreDb.Create(projectId);

            //Create a query
            CollectionReference collection = db.Collection("cities");
            Query qref = collection.Where("Capital", QueryOperator.Equal, true);

            //Listen to realtime updates
            FirebaseDocumentListener listener = qref.AddSnapshotListener();

            //Listen to document changes
            listener.DocumentChanged += (obj, e) =>
            {
                var city = e.DocumentSnapshot.Deserialize<City>();
                Console.WriteLine(string.Format("City {0} Changed/Added with pop {1}", city.Name, city.Population));
            };

            //Listen to errors
            listener.Error += (obj, e) =>
            {
                Console.WriteLine("Error => " + e.Message);
            };

            //Setup some other event handlers that may be interesting
            listener.Current += (obj, e) =>
            {
                Console.WriteLine(string.Format("Listener is current - {0:O}", DateTime.Now.ToUniversalTime()));
            };
            listener.DocumentRemoved += (obj, e) =>
            {
                Console.WriteLine("Document Removed " + e.Id);
            };
            listener.DocumentDeleted += (obj, e) =>
            {
                Console.WriteLine("Document Deleted " + e.Id);
            };

            //Cleanup and Exit program if any key is pressed
            Console.WriteLine("Press any key to quit");
            Console.ReadKey();
            Console.WriteLine("Canceling the listener");
            listener.Cancel();
            Console.WriteLine("Goodbye!");
        }
    }
}

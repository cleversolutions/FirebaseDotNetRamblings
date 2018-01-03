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
    //export GOOGLE_APPLICATION_CREDENTIALS="C:\Users\evanm\dev\FirebaseProfiles\Playing With Firestore-39da6bb7da7a.json"
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
            FirebaseDocumentListener listener = qref.AddSnapshotListener(); //new FirebaseDocumentListener(db);

            //Setup some event handlers on that listener
            listener.Error += (obj, e) =>{
                Console.WriteLine("Error => " + e.Message);
            };
            listener.Current += (obj, e) =>
            {
                Console.WriteLine(string.Format("Listener is current - {0:O}", DateTime.Now.ToUniversalTime()));
            };
            listener.DocumentChanged += (obj, e) =>
            {
                var city = e.DocumentSnapshot.Deserialize<City>();
                Console.WriteLine(string.Format("City {0} Changed/Added with pop {1}", city.Name, city.Population ));
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

        /**** Anything below here is just some ramblings of a crazy man. Pay it no attention, I just hate deleting old code *****/


        // //Setup our query
        // var query = new StructuredQuery
        // {
        //     From = { new CollectionSelector { CollectionId = "cities" } },
        //     Where = new Filter
        //     {
        //         FieldFilter = new FieldFilter
        //         {
        //             Field = new FieldReference { FieldPath = "Capital" },
        //             Op = FieldFilter.Types.Operator.Equal,
        //             Value = new Value { BooleanValue = true }
        //         }
        //     }
        // };
        // //Listen to changes to the query
        // listener.ListenToQuery(query);

        static async Task MainAsync(string[] args)
        {
            Console.WriteLine("Hello World!");

            FirestoreDb db = FirestoreDb.Create(projectId);
            CollectionReference collection = db.Collection("cities");
            Query qref = collection.Where("Capital", QueryOperator.Equal, true);
            
            QuerySnapshot qs = await qref.SnapshotAsync();
            foreach (var item in qs.Documents)
            {
                var city = item.Deserialize<City>();
                Console.WriteLine(String.Format("{0} is a capital", city.Name));
            }

            //FirestoreDb db = FirestoreDb.Create(projectId);

            //await ListenRequestTest();
            //await ListenTest();

            //await CreateCity();

            //await QueryFirestore();

            //await Test1();

            // Create a document with a random ID in the "users" collection.
            //CollectionReference collection = db.Collection("users");
            //DocumentReference document = await collection.AddAsync(new { Name = new { First = "Ada", Last = "Lovelace" }, Born = 1815 });

            // // A DocumentReference doesn't contain the data - it's just a path.
            // // Let's fetch the current document.
            // DocumentSnapshot snapshot = await document.SnapshotAsync();

            // // We can access individual fields by dot-separated path
            // Console.WriteLine(snapshot.GetField<string>("Name.First"));
            // Console.WriteLine(snapshot.GetField<string>("Name.Last"));
            // Console.WriteLine(snapshot.GetField<int>("Born"));

            // Query the collection for all documents where doc.Born < 1900.
            // Query query = collection.Where("Born", QueryOperator.LessThan, 1900);
            // QuerySnapshot querySnapshot = await query.SnapshotAsync();
            // foreach (DocumentSnapshot queryResult in querySnapshot.Documents)
            // {
            //     string firstName = queryResult.GetField<string>("Name.First");
            //     string lastName = queryResult.GetField<string>("Name.Last");
            //     int born = queryResult.GetField<int>("Born");
            //     Console.WriteLine($"{firstName} {lastName}; born {born}");
            // }
        }

        static async Task Test1()
        {
            //await GetDocumentById();
            //await ListDocumentsAsync_RequestObject();
            await RunQuery();
        }

        public static async Task ListDocumentsAsync_RequestObject()
        {
            FirestoreDb db = FirestoreDb.Create(projectId);
            var citiesPath = string.Format("projects/{0}/databases/{1}/documents", projectId, db.DatabaseId);
            // Snippet: ListDocumentsAsync(ListDocumentsRequest,CallSettings)
            // Create client
            FirestoreClient firestoreClient = await FirestoreClient.CreateAsync();
            // Initialize request argument(s)
            ListDocumentsRequest request = new ListDocumentsRequest
            {
                //Parent = AnyPathName.Parse(citiesPath).ToString(),
                //Parent = new AnyPathName(projectId, db.DatabaseId, "", "").ToString(),
                Parent = citiesPath,
                CollectionId = "cities",
            };

            // Make the request
            PagedAsyncEnumerable<ListDocumentsResponse, Document> response =
                firestoreClient.ListDocumentsAsync(request);

            Console.WriteLine(string.Format("Requesting: {0}", request.Parent));
            Console.WriteLine("Got {0} items", await response.Count());

            // Iterate over all response items, lazily performing RPCs as required
            await response.ForEachAsync((Document item) =>
            {
                // Do something with each item
                Console.WriteLine("item");
            });

            // Or iterate over pages (of server-defined size), performing one RPC per page
            await response.AsRawResponses().ForEachAsync((ListDocumentsResponse page) =>
            {
                // Do something with each page of items
                Console.WriteLine("A page of results:");
                foreach (Document item in page)
                {
                    Console.WriteLine("Got a document");
                }
            });

            // Or retrieve a single page of known size (unless it's the final page), performing as many RPCs as required
            int pageSize = 10;
            Page<Document> singlePage = await response.ReadPageAsync(pageSize);
            // Do something with the page of items
            Console.WriteLine($"A page of {pageSize} results (unless it's the final page):");
            foreach (Document item in singlePage)
            {
                Console.WriteLine(item);
            }
            // Store the pageToken, for when the next page is required.
            string nextPageToken = singlePage.NextPageToken;
            // End snippet
        }

        /// <summary>Snippet for RunQuery</summary>
        public static async Task RunQuery()
        {
            FirestoreDb db = FirestoreDb.Create(projectId);
            var citiesPath = string.Format("projects/{0}/databases/{1}/documents", projectId, db.DatabaseId);

            // Snippet: RunQuery(RunQueryRequest,CallSettings)
            // Create client
            FirestoreClient firestoreClient = FirestoreClient.Create();

            // Initialize request argument
            RunQueryRequest request = new RunQueryRequest
            {
                Parent = citiesPath,
            };
            // Make the request, returning a streaming response
            FirestoreClient.RunQueryStream streamingResponse = firestoreClient.RunQuery(request);

            // Read streaming responses from server until complete
            IAsyncEnumerator<RunQueryResponse> responseStream = streamingResponse.ResponseStream;
            while (await responseStream.MoveNext())
            {
                RunQueryResponse response = responseStream.Current;
                Console.WriteLine(response.Document);
                // Do something with streamed response
            }
            // The response stream has completed
            // End snippet
        }

        static async Task GetDocumentById()
        {
            FirestoreDb db = FirestoreDb.Create(projectId);

            // Snippet: GetDocumentAsync(GetDocumentRequest,CallSettings)
            // Create client
            FirestoreClient firestoreClient = await FirestoreClient.CreateAsync();
            // Initialize request argument(s)
            GetDocumentRequest request = new GetDocumentRequest
            {
                Name = new AnyPathName(projectId, db.DatabaseId, "cities", "9n3yeBkGYQz8VVFWXcUd").ToString(),
            };
            // Make the request
            Document response = await firestoreClient.GetDocumentAsync(request);


            Console.WriteLine(string.Format("Recieved: {0}", string.Join(",", response.Fields.Keys.Select(k => k))));
            // End snippet
        }


        // static async Task ListenTest(){
        //     FirestoreClient firestoreClient = FirestoreClient.Create();
        //     FirestoreClient.ListenStream duplexStream = firestoreClient.Listen();

        // }

        static async Task ListenRequestTest()
        {
            FirestoreDb db = FirestoreDb.Create(projectId);
            string databaseId = db.DatabaseId;

            // Create client
            FirestoreClient firestoreClient = FirestoreClient.Create();


            //Listen Settings


            // Initialize streaming call, retrieving the stream object
            FirestoreClient.ListenStream duplexStream = firestoreClient.Listen();

            // Sending requests and retrieving responses can be arbitrarily interleaved.
            // Exact sequence will depend on client/server behavior.

            using (BlockingCollection<TargetChange> tokenQueue = new BlockingCollection<TargetChange>())
            {
                //TargetChange lastResume = null;
                // Create task to do something with responses from server
                Task responseHandlerTask = Task.Run(async () =>
                {
                    IAsyncEnumerator<ListenResponse> responseStream = duplexStream.ResponseStream;
                    try
                    {
                        while (await responseStream.MoveNext())
                        {
                            ListenResponse response = responseStream.Current;
                            if (response.TargetChange != null && response.TargetChange.ResumeToken != null && response.TargetChange.ResumeToken.Length > 0)
                            {
                                Console.WriteLine(string.Format("Recieved Resume Token"));
                                Console.WriteLine(response.TargetChange);
                                //tokenQueue.Add(response.TargetChange);
                                //lastResume = response.TargetChange;
                            }
                            else if (response.DocumentChange != null)
                            {
                                Console.WriteLine("Received Change");
                                Console.WriteLine(response.DocumentChange.Document);
                            }
                            else
                            {
                                Console.WriteLine("Unknown Response");
                                Console.WriteLine(response);
                            }
                        }
                    }
                    catch (System.AggregateException)
                    {
                        Console.WriteLine("Received RST_STREAM Attempting resume");
                        // if(lastResume != null){
                        //     //try to resume
                        //     var resumeRequest = new ListenRequest
                        //     {
                        //         Database = new DatabaseRootName(projectId, databaseId).ToString(),
                        //         AddTarget = new Target { ResumeToken = lastResume.ResumeToken, ReadTime = lastResume.ReadTime }
                        //     };
                        //     // Stream a request to the server
                        //     await duplexStream.WriteAsync(resumeRequest);
                        // }
                    }
                    // The response stream has completed
                    Console.WriteLine("The response stream has completed");
                });

                // Send requests to the server
                // bool done = false;
                // while (!done)
                // {

                Console.WriteLine(string.Format("Trying to listen to project {0} database: {1}", projectId, databaseId));
                var citiesPath = string.Format("projects/{0}/databases/{1}/documents/cities/9n3yeBkGYQz8VVFWXcUd", projectId, databaseId);
                //projects/playing-with-firestore-162c9/databases/(default)/documents/cities
                // Initialize a request
                var dt = new DocumentsTarget { };
                dt.Documents.Add(citiesPath);
                // var docPath = new Google.Protobuf.Collections.RepeatedField<string>();
                // docPath.Add(citiesPath);
                ListenRequest request = new ListenRequest
                {
                    Database = new DatabaseRootName(projectId, databaseId).ToString(),
                    AddTarget = new Target
                    {
                        Documents = dt
                    }
                };
                // Stream a request to the server
                await duplexStream.WriteAsync(request);

                //Respond to resume requests (up to 100 of them to limit the damage we can do to our quota)
                for (int i = 0; i < 10; i++)
                {
                    var targetChange = tokenQueue.Take();
                    var resumeRequest = new ListenRequest
                    {
                        Database = new DatabaseRootName(projectId, databaseId).ToString(),
                        AddTarget = new Target { ResumeToken = targetChange.ResumeToken, ReadTime = targetChange.ReadTime }
                    };
                    //await duplexStream.WriteAsync(resumeRequest);
                    //Console.WriteLine("Sent resumeToken");
                    // var resumeRequest = new WriteRequest();
                    // resumeRequest.StreamId = resumeToken;
                    // await duplexStream.WriteAsync(resumeRequest);
                }
                //Console.WriteLine("Completed Write Loop");
                // Set "done" to true when sending requests is complete
                //     done = true;
                // }
                // Complete writing requests to the stream
                //Console.WriteLine("Awaiting WriteCompleteAsync");

                //await duplexStream.WriteCompleteAsync();

                // Await the response handler.
                // This will complete once all server responses have been processed.
                Console.WriteLine("Awaiting responseHandlerTask");
                await responseHandlerTask;
                Console.WriteLine("All Done");
            }
        }


        static async Task ListenTest(ByteString resumeToken = null)
        {
            // Create client
            FirestoreClient firestoreClient = FirestoreClient.Create();

            //Setup no expiration for the listen
            CallSettings listenSettings = CallSettings.FromCallTiming(CallTiming.FromExpiration(Expiration.None));

            // Initialize streaming call, retrieving the stream object
            FirestoreClient.ListenStream duplexStream = firestoreClient.Listen(listenSettings);

            //Confirm the settings are taken
            ByteString lastResume = null;

            BlockingCollection<ListenRequest> requests = new BlockingCollection<ListenRequest>();

            //bool done = false;

            // Create task to do something with responses from server
            Task responseHandlerTask = Task.Run(async () =>
            {
                IAsyncEnumerator<ListenResponse> responseStream = duplexStream.ResponseStream;
                try
                {
                    while (await responseStream.MoveNext())
                    {
                        ListenResponse response = responseStream.Current;
                        if (response.TargetChange != null && response.TargetChange.ResumeToken != null && response.TargetChange.ResumeToken.Length > 0)
                        {
                            Console.WriteLine(response.TargetChange.ResumeToken.ToBase64());
                            lastResume = response.TargetChange.ResumeToken;
                        }
                        Console.WriteLine(response);
                    }
                }
                catch (Grpc.Core.RpcException ex)
                {
                    Console.WriteLine("Handling Exception...");
                    Console.WriteLine(ex);

                    var status = duplexStream.GrpcCall.GetStatus();
                    Console.WriteLine(string.Format("Exception: stream status {0} - {1}", status.StatusCode.ToString(), status.Detail));
                    //Can we restart it?
                    //See https://github.com/googleapis/nodejs-firestore/blob/ed83393ac9f646e33f429485a8e0ddcdd77ecb84/src/watch.js
                    if (ex.Status.StatusCode == Grpc.Core.StatusCode.Internal)
                    {
                        Console.WriteLine("Attempting to recover");

                        await ListenTest(lastResume);
                        //TODO -- rework this so it's not in a stack -- this will overflow eventually
                    }
                }
            });

            // Send requests to the server
            var citiesPath = string.Format("projects/{0}/databases/{1}/documents/cities/9n3yeBkGYQz8VVFWXcUd", projectId, databaseId);

            // Initialize a request
            var dt = new DocumentsTarget { };
            dt.Documents.Add(citiesPath);

            ListenRequest request = new ListenRequest
            {
                Database = new DatabaseRootName(projectId, databaseId).ToString(),
                AddTarget = new Target
                {
                    Documents = dt
                }
            };

            if (resumeToken != null)
            {
                Console.WriteLine(string.Format("Resuming a listen with token {0}", resumeToken.ToBase64()));
                request.AddTarget.ResumeToken = resumeToken;
            }

            //Write out requsts 
            //while(!done){
            for (int i = 0; i < 10; i++)
            {
                var newRequest = requests.Take();
                // Stream a request to the server
                await duplexStream.WriteAsync(newRequest);
            }

            await duplexStream.WriteCompleteAsync();

            // Await the response handler.
            // This will complete once all server responses have been processed.
            await responseHandlerTask;

            if (lastResume != null && lastResume.Length > 0)
            {
                await ListenTest(lastResume);
            }
        }


        static async Task CreateCity()
        {

            FirestoreDb db = FirestoreDb.Create(projectId);

            Console.WriteLine(string.Format("Creating Cities in project: {0}", db.ProjectId));


            // You can create references directly from FirestoreDb:
            CollectionReference citiesFromDb = db.Collection("cities");
            DocumentReference londonFromDb = db.Document("cities/london");
            CollectionReference londonRestaurantsFromDb = db.Collection("cities/london/restaurants");

            // Or from other references:
            DocumentReference londonFromCities = citiesFromDb.Document("london");
            CollectionReference londonRestaurantFromLondon = londonFromDb.Collection("restaurants");

            CollectionReference collection = db.Collection("cities");
            City city = new City
            {
                Name = "Los Angeles",
                Country = "USA",
                State = "CA",
                IsCapital = false,
                Population = 3900000L
            };
            // Alternatively, collection.Document("los-angeles").Create(city);
            DocumentReference document = await collection.AddAsync(city);

            City newCity = new City
            {
                Name = "Los Angeles",
                Country = "United States of America",
                Population = 3900005L
            };
            await document.SetAsync(newCity, SetOptions.MergeFields("Country", "Population"));

            await document.SetAsync(new { Name = "Test", Country = "United States of America", Population = 3900005L }, SetOptions.MergeAll);

            DocumentSnapshot snapshot = await document.SnapshotAsync();
            // Even if there's no document in the server, we still get a snapshot
            // back - but it knows the document doesn't exist.
            Console.WriteLine(snapshot.Exists);

            // Individual fields can be checked and fetched
            Console.WriteLine(snapshot.Contains("Planet")); // False
            Console.WriteLine(snapshot.GetField<string>("Name")); // Los Angeles

            // Or you can deserialize to a dictionary or a model
            City fetchedCity = snapshot.Deserialize<City>();
            Console.WriteLine(fetchedCity.Name); // Los Angeles
        }

        static async Task QueryFirestore()
        {
            FirestoreDb db = FirestoreDb.Create(projectId);
            CollectionReference collection = db.Collection("cities");

            // A CollectionReference is a Query, so we can just fetch everything
            QuerySnapshot allCities = await collection.SnapshotAsync();

            foreach (DocumentSnapshot document in allCities.Documents)
            {
                // Do anything you'd normally do with a DocumentSnapshot
                City city = document.Deserialize<City>();
                Console.WriteLine(city.Name);
            }

        }


    }
}

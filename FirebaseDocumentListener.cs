using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Api.Gax.Grpc;
using Google.Cloud.Firestore.V1Beta1;
using Google.Protobuf;
using Grpc.Core;
using static Google.Cloud.Firestore.V1Beta1.Target.Types;
using static Google.Cloud.Firestore.V1Beta1.TargetChange.Types;

/*
 *  An example document listener for firebase
 *  
 *  Firebase will terminate our listen request every 5 minutes, so we need a system to resend our requests so they pick up
 *  where they last left off. To accomplish this, I use a BlockingCollection, a status flags and some threads. 
 */
public class FirebaseDocumentListener
{
    private bool Done = false;
    private FirestoreClient.ListenStream DuplexStream;
    private FirestoreClient FirestoreClient;
    private CallSettings ListenSettings;
    public string ProjectId { get; set; }
    public string DatabaseId { get; set; }
    private bool ListenerIsActive = false;
    private BlockingCollection<ListenRequest> PendingRequests = new BlockingCollection<ListenRequest>();
    private Queue<ListenRequest> ActiveRequests = new Queue<ListenRequest>();
    private Task RequestHanderTask = null;
    private Task ResponseHanderTask = null;
    private CancellationTokenSource CancellationTokenSource;
    private CancellationToken CancellationToken;

    public FirebaseDocumentListener()
    {
        // Create client
        FirestoreClient = FirestoreClient.Create();

        //Setup no expiration for the listen
        ListenSettings = CallSettings.FromCallTiming(CallTiming.FromExpiration(Expiration.None));

        RequestHanderTask = StartRequestHandlerTask();

        CancellationTokenSource = new CancellationTokenSource();
        CancellationToken = CancellationTokenSource.Token;
    }

    public void Cancel()
    {
        Done = true;
        this.CancellationTokenSource.Cancel();
    }

    public void ListenToDocument(string documentPath)
    {
        // Initialize a request
        var dt = new DocumentsTarget { };
        dt.Documents.Add(documentPath);

        ListenRequest request = new ListenRequest
        {
            Database = new DatabaseRootName(ProjectId, DatabaseId).ToString(),
            AddTarget = new Target
            {
                Documents = dt
            }
        };
        PendingRequests.Add(request);
    }

    public void ListenToQuery(StructuredQuery query){
        var qt = new QueryTarget{ };
        qt.StructuredQuery = query;
        qt.Parent = "projects/playing-with-firestore/databases/(default)/documents";
        
        ListenRequest request = new ListenRequest{
            Database = new DatabaseRootName(ProjectId, DatabaseId).ToString(),
            AddTarget = new Target{
                Query = qt,
            }  
        };
        PendingRequests.Add(request);
    }

    private void AddRequest(ListenRequest request)
    {
        PendingRequests.Add(request);
    }

    private bool IsPermanentError(RpcException exception)
    {
        if (exception == null) return false;
        switch (exception.Status.StatusCode)
        {
            case StatusCode.Cancelled:
            case StatusCode.Unknown:
            case StatusCode.DeadlineExceeded:
            case StatusCode.ResourceExhausted:
            case StatusCode.Internal:
            case StatusCode.Unavailable:
            case StatusCode.Unauthenticated:
                return false;
            default:
                return true;
        }
    }



    private Task StartRequestHandlerTask()
    {
        Console.WriteLine("Started Request Handler");
        return Task.Run(async () =>
        {
            while (!Done)
            {
                var request = PendingRequests.Take(CancellationToken);
                
                Console.WriteLine("Setup listen for request");

                //If the listener isn't active, start it
                if (DuplexStream == null || !ListenerIsActive)
                {
                    // Initialize streaming call, retrieving the stream object
                    DuplexStream = FirestoreClient.Listen(ListenSettings);
                    ListenerIsActive = true;

                    Console.WriteLine("Response Task Not Active, starting");
                    ResponseHanderTask = StartResponseHandlerTask();
                }
                Console.WriteLine("Sending Request");
                // Stream a request to the server
                await DuplexStream.WriteAsync(request);
                ActiveRequests.Enqueue(request);
            }
            Console.WriteLine("Request Handler Completed");
            await DuplexStream.WriteCompleteAsync();
        });
    }

    private void RestartAllRequests()
    {
        Console.WriteLine("Restarting Requests");
        while(ActiveRequests.Count > 0){
            PendingRequests.Add(ActiveRequests.Dequeue());
        }
    }

    //TODO -- we need to figure out how to respond to many different respons and target changes
    //Look at line 690 on https://github.com/googleapis/nodejs-firestore/blob/ed83393ac9f646e33f429485a8e0ddcdd77ecb84/src/watch.js
    private Task StartResponseHandlerTask()
    {
        Console.WriteLine("Starting Response Handler");
        return Task.Run(async () =>
        {
            IAsyncEnumerator<ListenResponse> responseStream = DuplexStream.ResponseStream;
            try
            {
                //ListenerIsActive = true;
                while (await responseStream.MoveNext(CancellationToken))
                {
                    ListenResponse response = responseStream.Current;
                    if (response.TargetChange != null)
                    {
                        Console.WriteLine(response);
                        if(response.TargetChange.TargetChangeType == TargetChangeType.NoChange){
                            Console.WriteLine("No Change -- todo update our resume token");
                        }else if(response.TargetChange.TargetChangeType == TargetChangeType.Add){
                            Console.WriteLine("Add -- todo, maybe this is a good place to add to active requests");
                        }else if(response.TargetChange.TargetChangeType == TargetChangeType.Remove){
                            Console.WriteLine("Remove -- todo shut down our listener");
                        }else if(response.TargetChange.TargetChangeType == TargetChangeType.Reset){
                            Console.WriteLine("Reset");
                        }else if(response.TargetChange.TargetChangeType == TargetChangeType.Current){
                            Console.WriteLine("Current -- we are up to date... maybe we can do something?..");
                        }else{
                            Console.WriteLine("Error ==> Unknown TargetChangeType");
                        }

                        if(response.TargetChange.ResumeToken != null && response.TargetChange.ResumeToken.Length > 0){
                            foreach(var request in ActiveRequests){
                                request.AddTarget.ResumeToken = response.TargetChange.ResumeToken;
                            }
                        }
                    }else if(response.DocumentChange != null){
                        Console.WriteLine("TODO -- Handle Document Change");
                        Console.WriteLine(response);
                    }else if(response.DocumentRemove != null){
                        Console.WriteLine("TODO -- Handle Document Remove");
                        Console.WriteLine(response);
                    }else if(response.DocumentDelete != null){
                        Console.WriteLine("TODO -- Handle Document Delete");
                        Console.WriteLine(response);
                    }else if(response.Filter != null){
                        Console.WriteLine("TODO -- Handle Filter");
                        Console.WriteLine(response);
                    }else{
                        Console.WriteLine("Error ==> Unknown ResponseType");
                    }
                }
            }
            catch (RpcException ex)
            {
                ListenerIsActive = false;

                var status = DuplexStream.GrpcCall.GetStatus();
                Console.WriteLine(string.Format("Handling Exception: stream status {0} - {1}", status.StatusCode.ToString(), status.Detail));

                if(CancellationToken.IsCancellationRequested){
                    Console.WriteLine("Cancel Requested - will not attempt recovery.");
                }
                else if (!IsPermanentError(ex))
                {
                    Console.WriteLine("Attempting to recover");
                    RestartAllRequests();
                }
            }
            Console.WriteLine("Response Handler Completed");
        });
    }

}
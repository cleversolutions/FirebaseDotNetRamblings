using Google.Cloud.Firestore;

[FirestoreData]
public class City
{
    [FirestoreProperty]
    public string Name { get; set; }

    [FirestoreProperty]
    public string State { get; set; }

    [FirestoreProperty]
    public string Country { get; set; }

    [FirestoreProperty("Capital")]
    public bool IsCapital { get; set; }

    [FirestoreProperty]
    public long Population { get; set; }
}

package com.amazonaws.samples;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Scanner;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.auth.InstanceProfileCredentialsProvider; //IAm role
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;//IAM role
import com.amazonaws.SdkClientException;

public class S3AWS {
    
    private static final String SUFFIX = "/";
    
    public static void main(String[] args) throws IOException,SdkClientException,AmazonServiceException{
        Scanner sc = new Scanner(System.in);
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        ClientConfiguration config = new ClientConfiguration();
        AWSCredentials credentials = new BasicAWSCredentials("","");
        credentials = new BasicAWSCredentials(
                "Your Access Key ID",
                "Your Secret Access Key");
        AmazonS3Client s3client;    
        
        s3client = new AmazonS3Client(credentials);
        

        
        int n = 1;
        while(n != 0)
        {
            System.out.println("Choose one of the following :- ");
            System.out.println("1. Create a Bucket");
            System.out.println("2. List all the buckets");
            System.out.println("3. Create a Folder in the Bucket");
            System.out.println("4. Upload a file in the Bucket");
            System.out.println("5. Delete a Folder in the Bucket");
            System.out.println("6. Delete a Bucket");
            System.out.println("0. Exit");

            n = sc.nextInt();
            
            switch(n) {
            case 1:
                System.out.print("Enter Bucket name :- ");
                String bucketName = br.readLine();
                System.out.println("");
                s3client.createBucket(bucketName);
                System.out.println("");
                break;
            case 2:
                for (Bucket bucket : s3client.listBuckets()) {
                    System.out.println(" - " + bucket.getName());
                }
                break;
            case 3:
                System.out.print("Enter Bucket Name :- ");
                String bucketname1 = br.readLine();
                System.out.println("");
                System.out.print("Enter Folder name :- ");
                String folderName1 = br.readLine();
                System.out.println("");
                System.out.println("Creating Folder......");
                createFolder(bucketname1, folderName1, s3client);
                break;
            case 4:
                System.out.print("Enter Bucket Name :- ");
                String bucketname2 = br.readLine();
                System.out.println("");
                String folderName2 = "upload";
                System.out.println("");
                System.out.print("Enter File name :- ");
                String file = br.readLine();
                System.out.println("");
                System.out.print("Enter Location of the file :- ");
                String location = br.readLine();
                System.out.println("");
                String fileName = folderName2 + SUFFIX + file;//folderName2 should not be taken from user.
                s3client.putObject(new PutObjectRequest(bucketname2, fileName,
                        new File(location))
                        .withCannedAcl(CannedAccessControlList.PublicRead));
                System.out.println("Upload Complete");
                break;
            case 5:
                System.out.print("Enter Bucket Name :- ");
                String bucketname3= br.readLine();
                System.out.println("");
                System.out.print("Enter Folder name :- ");
                String foldername3 = br.readLine();
                System.out.println("");
                System.out.println("Deleting Folder......");
                deleteFolder(bucketname3, foldername3, s3client);
                break;
            case 6:
                System.out.print("Enter Bucket's Name :- ");
                String bucketname4 = br.readLine();
                System.out.println("");
                System.out.println("Deleting Bucket......");
                s3client.deleteBucket(bucketname4);
                break;
            case 0:
                System.out.println("Thank you");
            default:
                System.out.println("Invalid Option");
                break;
            }
        }

    }
    
    
    public static void createFolder(String bucketName, String folderName, AmazonS3 client) {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(0);
        InputStream emptyContent = new ByteArrayInputStream(new byte[0]);
        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName,
                folderName + SUFFIX, emptyContent, metadata);
        client.putObject(putObjectRequest);
    }
    
    public static void deleteFolder(String bucketName, String folderName, AmazonS3 client) {
        List<S3ObjectSummary> fileList =
                client.listObjects(bucketName, folderName).getObjectSummaries();
        for (S3ObjectSummary file : fileList) {
            client.deleteObject(bucketName, file.getKey());
        }
        client.deleteObject(bucketName, folderName);
    }
}

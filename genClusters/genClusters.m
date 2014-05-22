%% genClusters.m
% Generating testing data for k-means clustering algorithm.
% Chen Wang
% chenw@cmu.edu, April, 20, 2014

clc;
clear all;
close all;

mn = rand(3, 10) .* 1000;

fileID = fopen('kmeans10M.dat', 'w');

total_num = 10000000;
id = 1;

while id < total_num

    normRnd = normrnd(mn, 5);
    
    for i = 1 : size(normRnd, 1)
        curVec = [id normRnd(i, :)];
        fprintf(fileID, ['%d\t', repmat('%8.4f\t',1,size(normRnd, 2) - 1),'%8.4f\n'], curVec); 
        id = id + 1;
    end

end

fclose(fileID);

-module(map_reduce).
-compile(export_all).

%% This function spawns reduce and maps processes and receives Result from reduce
start(FileList) ->
  ReducePID = spawn(map_reduce, reduce, [self(), 0, length(FileList), #{}]),
  lists:foreach(fun(File) -> spawn(map_reduce, map, [ReducePID, File]) end, FileList), % each file have own map
  receive
		Result ->
      Result
	end.

%% Map function that try to open file and after word counting sends one file MapResult to reduce process
map(ReducePID, FileName) ->
  case file:open(filename:join(["../data", FileName]), [read, {encoding, unicode}]) of
        {ok, IoDevice} ->
          MapResult = count_words(IoDevice, #{}),
          ReducePID ! {MapResult};
        {error, _} ->
          ReducePID ! {error_file_read, FileName},
          exit("Cant read the file")
  end.

%% Reduce process waits for messages from maps and compares the number of files and the number of mapped files
%% If all files processed (maybe with errors of reading) reduce would send Results to the main process
%% Function is merging received MapResults with merged map of previous ones
%% After that reduce updates number of processed files and recursively calls itself
reduce(MainPID, FilesCount, FilesNumber, ReduceAllFiles) ->
  if FilesCount =:= FilesNumber ->
    MainPID ! ReduceAllFiles;
  true ->
    receive
      {MapResult} ->
        reduce(MainPID, FilesCount + 1, FilesNumber,
                                      maps_merge(ReduceAllFiles, MapResult));
      {error_file_read, FileName} ->
        io:fwrite(["Cant read the file ", FileName ]),
        reduce(MainPID, FilesCount + 1, FilesNumber, ReduceAllFiles)
    end
  end.

%% Maps merging with the addition of values of same keys
maps_merge(Map1, Map2) ->
  maps:fold(fun(K, V, Map) ->
              maps:update_with(K, fun(X) -> X + V end, V, Map) end, Map1, Map2).

%% Recursive words counting line by line
count_words(IoDevice, MapFile) ->
    case io:get_line(IoDevice, "") of
      eof ->
        file:close(IoDevice),
        MapFile;
      {error, Reason} ->
        file:close(IoDevice),
        {error_line_read, Reason};
      Line ->
        Words = string:lexemes(Line, " $\n"),
        MapLine = count_words_in_line(Words, #{}),
        MapFileNew = maps_merge(MapFile, MapLine),
        count_words(IoDevice, MapFileNew)
    end.

  count_words_in_line([H|T], MapCounter) ->
     Count = maps:get(H, MapCounter, 0),
     count_words_in_line(T, maps:put(H, Count + 1, MapCounter));
  count_words_in_line([], MapCounter) ->
      MapCounter.

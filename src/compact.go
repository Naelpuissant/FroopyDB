package src

// func (db *DB) Set(key int, value string) string {
// 	// Sould be refactored
// 	if db.seg.lines+1 > db.segmentMaxSize {
// 		newSegmentLines := db.compactSegments([]*os.File{db.seg.file}, db.seg.name)
// 		// while compacting I should write on new segment
// 		// and then merging so compact is non blocking
// 		newSegment := db.segments.AddNew()
// 		prevSegment := db.seg

// 		// add to new seg
// 		db.seg = newSegment
// 		db.seg.Open()

// 		line := db.seg.Add(key, value)

// 		// can merge
// 		if newSegmentLines+1 <= db.segmentMaxSize {
// 			// once I did the non blocking compact
// 			// I should be aware that the new segment
// 			// might be more than 1 line
// 			newSegmentLines = db.compactSegments([]*os.File{prevSegment.file, db.seg.file}, prevSegment.name)

// 			db.segments.Remove(newSegment)
// 			db.seg.lines = newSegmentLines
// 			db.seg = prevSegment
// 		}
// 		return line
// 	}
// 	line := db.seg.Add(key, value)
// 	return line
// }

// func compactSegments(segments []*os.File, target string) int {
// 	compactedSegment := map[int]string{}

// 	for _, segment := range segments {
// 		segment.Seek(0, 0)
// 		scanner := bufio.NewScanner(segment)
// 		for scanner.Scan() {
// 			line := strings.Split(scanner.Text(), ":")

// 			currKey, _ := strconv.Atoi(line[0])
// 			currValue := line[1]
// 			if currValue == "\x00" {
// 				delete(compactedSegment, currKey)
// 			} else {
// 				compactedSegment[currKey] = currValue
// 			}
// 		}
// 	}

// 	// persist in new tmp file
// 	tmpSegmentName := fmt.Sprintf("%s_tmp", target)
// 	tmpSegment := NewSegment(tmpSegmentName, 0)
// 	tmpSegment.Open()

// 	for key, value := range compactedSegment {
// 		tmpSegment.Add(key, value)
// 	}

// 	tmpSegment.Rename(target)
// 	db.segments.Replace(db.seg, tmpSegment)
// 	db.seg = tmpSegment

// 	return db.seg.lines
// }

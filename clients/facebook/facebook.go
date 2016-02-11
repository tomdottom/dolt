package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/d"
	"github.com/attic-labs/noms/dataset"
	"github.com/attic-labs/noms/types"
	"golang.org/x/oauth2"
)

const facebookAPI = "https://graph.facebook.com/"

var (
	authHTTPClient    *http.Client
	cachingHTTPClient *http.Client
	ds                *dataset.Dataset
	tokenFlag         = flag.String("token", "", "Facebook auth token (required) - see usage for instructions")
	start             time.Time
)

func main() {
	flag.Usage = usage
	dsFlags := dataset.NewFlags()
	flag.Parse()
	cachingHTTPClient = util.CachingHttpClient()

	if *tokenFlag == "" || cachingHTTPClient == nil {
		flag.Usage()
		return
	}

	ds = dsFlags.CreateDataset()
	if ds == nil {
		flag.Usage()
		return
	}
	defer ds.Store().Close()

	token := oauth2.Token{AccessToken: *tokenFlag}
	authHTTPClient = oauth2.NewClient(oauth2.NoContext, oauth2.StaticTokenSource(&token))

	start = time.Now()
	var user = getUser()
	printStats(user)

	userRef := types.WriteValue(user, ds.Store())
	fmt.Printf("userRef: %s\n", userRef)
	_, err := ds.Commit(NewRefOfUser(userRef))
	d.Exp.NoError(err)
}

func usage() {
	credentialSteps := `To get an oauth token:
1. Browse to: https://developers.facebook.com/tools/explorer/
2. Login with your Facebook credentialSteps
3. In the 'Get Token' dropdown menu, select 'Get User Access Token'
4. Copy the Access Token from the textbox
`
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	flag.PrintDefaults()
	fmt.Fprintf(os.Stderr, "\n%s\n\n", credentialSteps)
}

func getUser() User {
	uj := UserJSON{}
	callFacebookAPI(authHTTPClient, facebookAPI+"v2.5/me", &uj)
	return NewUser().
		SetId(uj.ID).
		SetName(uj.Name).
		SetPhotos(getPhotos())
}

func getPhotos() SetOfRefOfRemotePhoto {
	photos := NewSetOfRefOfRemotePhoto()
	url := facebookAPI + "v2.5/me/photos/uploaded?limit=1000&fields=fields=place,name,created_time,images,tags{x,y,name}&date_format=U"

	for url != "" {
		plj := PhotoListJSON{}
		callFacebookAPI(authHTTPClient, url, &plj)
		fmt.Printf("Found %d photos\n", len(plj.Data))
		for _, entry := range plj.Data {
			photo := RemotePhotoDef{
				Id:    entry.Id,
				Title: entry.Name,
				Date:  DateDef{MsSinceEpoch: int64(entry.CreatedTime) * 1000},
				Geoposition: GeopositionDef{
					Latitude:  entry.Place.Location.Latitude,
					Longitude: entry.Place.Location.Longitude,
				},
			}.New()

			photo = photo.SetSizes(getSizes(entry.Images))
			photo = photo.SetFaces(getFaces(entry.Tags,
				float32(entry.Images[0].Width),
				float32(entry.Images[0].Height)))

			photos = photos.Insert(NewRefOfRemotePhoto(types.WriteValue(photo, ds.Store())))
		}

		url = plj.Paging.Next
	}

	return photos
}

func getSizes(images []ImageJSON) (result MapOfSizeToString) {
	result = NewMapOfSizeToString()
	for _, img := range images {
		result = result.Set(
			SizeDef{Width: img.Width, Height: img.Height}.New(),
			img.Source)
	}
	return
}

func getFaces(tags TagListJSON, width, height float32) (result SetOfFace) {
	// Facebook sadly doesn't give us the bounding box, only the center point.
	// We could fix this by using OpenCV to do face detection, and then matching the found rectangles with the data from FB.
	// See: https://github.com/lazywei/go-opencv
	// But for now, we assert that the bounding boxes are always 20% of the longer edge of the image.
	const faceSize = 0.2
	var faceW, faceH float32
	if width > height {
		faceW = faceSize
		faceH = height / width * faceSize
	} else {
		faceH = faceSize
		faceW = width / height * faceSize
	}

	result = NewSetOfFace()
	for _, tag := range tags.Data {
		if tag.X == 0 && tag.Y == 0 {
			continue
		}
		result = result.Insert(FaceDef{
			Top:        tag.Y/100 - (faceH / 2),
			Left:       tag.X/100 - (faceW / 2),
			Width:      faceW,
			Height:     faceH,
			PersonName: tag.Name,
		}.New())
	}
	return
}

func printStats(user User) {
	fmt.Printf("Imported %d photo(s), time: %.2f\n", user.Photos().Len(), time.Now().Sub(start).Seconds())
}

func callFacebookAPI(client *http.Client, url string, response interface{}) {
	fmt.Printf("Fetching %s...\n", url)
	req, err := http.NewRequest("GET", url, nil)
	d.Chk.NoError(err)

	resp, err := client.Do(req)
	d.Chk.NoError(err)

	msg := func() string {
		body := &bytes.Buffer{}
		_, err := io.Copy(body, resp.Body)
		d.Chk.NoError(err)
		return fmt.Sprintf("could not load %s: %d: %s", url, resp.StatusCode, body)
	}

	switch resp.StatusCode / 100 {
	case 4:
		d.Exp.Fail(msg())
	case 5:
		d.Chk.Fail(msg())
	}

	err = json.NewDecoder(resp.Body).Decode(response)
	d.Chk.NoError(err)
}

"""Adds Listenbrainz support to Beets."""

import datetime

import musicbrainzngs
import requests

from beets import config, ui, dbcore
from beets.dbcore import types
from beets.plugins import BeetsPlugin
from beetsplug.lastimport import process_tracks


class ListenBrainzPlugin(BeetsPlugin):
    """A Beets plugin for interacting with ListenBrainz."""

    data_source = "ListenBrainz"
    ROOT = "http://api.listenbrainz.org/1/"

    def __init__(self):
        """Initialize the plugin."""
        super().__init__()
        self.token = self.config["token"].get()
        self.username = self.config["username"].get()
        self.AUTH_HEADER = {"Authorization": f"Token {self.token}"}
        config["listenbrainz"]["token"].redact = True

        self.item_types = {
            'listen_count': types.INTEGER,
        }

    def commands(self):
        """Add beet UI commands to interact with ListenBrainz."""
        lbupdate_cmd = ui.Subcommand(
            "lbimport", help=f"Import {self.data_source} history"
        )

        def func(lib, opts, args):
            self._lbupdate(lib, self._log)

        lbupdate_cmd.func = func

        updatelistens_cmd = ui.Subcommand(
            'lbupdatelistens', help=f'Fetch top track entries from {self.data_source} and update listen_count')
        updatelistens_cmd.parser.add_option(
            '-c', '--count', type='int', dest='count',
            help='number of entries to fetch')
        updatelistens_cmd.parser.add_option(
            '-o', '--offset', type='int', dest='offset',
            help='number of entries to skip from the beginning')
        def updatelistens_func(lib, opts, args):
            opt_count = opts.count if opts.count is not None else None
            opt_offset = opts.offset if opts.offset is not None else None
            self._lb_update_listens(lib, self._log, count=opt_count, offset=opt_offset)
        updatelistens_cmd.func = updatelistens_func

        return [lbupdate_cmd, updatelistens_cmd]

    def _lbupdate(self, lib, log):
        """Obtain view count from Listenbrainz."""
        found_total = 0
        unknown_total = 0
        ls = self.get_listens()
        tracks = self.get_tracks_from_listens(ls)
        log.info(f"Found {len(ls)} listens")
        if tracks:
            found, unknown = process_tracks(lib, tracks, log)
            found_total += found
            unknown_total += unknown
        log.info("... done!")
        log.info("{0} unknown play-counts", unknown_total)
        log.info("{0} play-counts imported", found_total)

    def _lb_update_listens(self, lib, log, count=None, offset=None):
        param_count = count
        param_offset = offset
        param_range = 'all_time'

        request_url = f'{self.ROOT}/stats/user/{self.username}/recordings'
        request_params = {
            k: v
            for k, v in {
                'count': param_count,
                'offset': param_offset,
                'range': param_range,
            }.items()
            if v is not None
        }
        response = self._make_request(request_url, request_params)
        if response is None:
            log.error(f'no response to request, url={request_url}, params={str(request_params)}')
            return

        response_payload = response['payload'] if 'payload' in response else None
        if response_payload is None:
            log.error(f'no payload in response: {str(response)}')
            return

        # Extract results from payload
        payload_count = response_payload['count']
        payload_offset = response_payload['offset']
        payload_range = response_payload['range']
        payload_recordings = response_payload['recordings']

        # Check for consistency, warn about any mismatches
        num_recordings = len(payload_recordings)
        if param_count is not None and (payload_count != param_count or num_recordings != param_count):
            log.warning(f'response count does not match request, param_count={param_count}, payload_count={payload_count}, num_recordings={num_recordings}')
        if param_offset is not None and payload_offset != param_offset:
            log.warning(f'response offset does not match request, param_offset={param_offset}, payload_offset={payload_offset}')
        if param_range is not None and payload_range != param_range:
            log.warning(f'response range does not match request, param_range={param_range}, payload_range={payload_range}')

        # Loop through recordings
        total_found = 0
        total_updated = 0
        for recording_idx in range(0, num_recordings):
            # Extract listen_count for current recording
            recording = payload_recordings[recording_idx]
            listen_count = None
            if 'listen_count' in recording:
                listen_count = int(recording['listen_count'])
                log.debug(f'for recording_idx={payload_offset}+{recording_idx}, got listen_count={listen_count}')
            if listen_count is None:
                log.warning(f'no listen_count in recording_idx={payload_offset}+{recording_idx}')
                continue

            # Use recording metadata to look up current song in beets library
            lib_song = None

            # Extract MusicBrainz ID for current recording
            recording_mbid = recording['recording_mbid'] if 'recording_mbid' in recording else None
            if recording_mbid is not None:
                query = dbcore.query.MatchQuery('mb_trackid', recording_mbid)
                lib_song = lib.items(query).get()

            # Fallback to looking up song using artist/album/track names
            if lib_song is None:
                log.debug('could not find song by MusicBrainz ID, falling back to title/artist/album names')
                track_name = recording['track_name'] if 'track_name' in recording else None
                if track_name is None:
                    log.error(f'cannot look up song with recording_mbid={recording_mbid} and track_name={track_name}')
                    continue

                artist = recording['artist_name'] if 'artist_name' in recording else None
                album = recording['release_name'] if 'release_name' in recording else None
                if artist is None and album is None:
                    log.error('cannot look up song with artist_name={artist} and release_name={album}')
                    continue

                # Construct and execute the query
                query_parts = [dbcore.query.SubstringQuery('title', track_name)]
                if artist is not None:
                    query_parts.append(dbcore.query.SubstringQuery('artist', artist))
                if album is not None:
                    query_parts.append(dbcore.query.SubstringQuery('album', album))
                query = dbcore.AndQuery(query_parts)
                lib_song = lib.items(query).get()

                if lib_song is None and album is not None and artist is not None:
                    # Look up using just artist/track names (i.e., do not include album name in the query)
                    log.debug('could not find song by title+artist+album, trying title+artist')
                    query = dbcore.AndQuery([
                        dbcore.query.SubstringQuery('title', track_name),
                        dbcore.query.SubstringQuery('artist', artist)
                    ])
                    lib_song = lib.items(query).get()

                    if lib_song is None:
                        # Look up using just album/track names (i.e., do not include artist name in the query)
                        log.debug('could not find song by title+artist, trying title+album')
                        query = dbcore.AndQuery([
                            dbcore.query.SubstringQuery('title', track_name),
                            dbcore.query.SubstringQuery('album', album)
                        ])
                        lib_song = lib.items(query).get()

            # Check whether we found a matching song item in the beets library
            if lib_song is None:
                log.error(f'could not find song for recording_idx={payload_offset}+{recording_idx}')
                continue
            log.debug(f'found song: {lib_song.artist} - {lib_song.album} - {lib_song.title}')
            total_found += 1

            # Check whether listen_count needs to be updated
            old_listen_count = int(lib_song.get('listen_count', 0))
            if listen_count <= old_listen_count:
                log.debug(f'no update needed to listen_count: {listen_count} <= {old_listen_count}')
                continue

            # TODO: what about summing up multiple listen_counts if different LB recordings refer to the same song in the beets library ???
            # TODO: this might be tricky, if someone wants to page through LB results in small batches (using count/offset) !!!

            # Update the listen_count attribute
            log.debug(f'updating listen_count: {old_listen_count} to {listen_count}')
            lib_song['listen_count'] = listen_count
            lib_song.store()
            total_updated += 1

        # Print a summary
        log.info(f'found {total_found} of {num_recordings} recordings, updated listen_count for {total_updated} items')

    def _make_request(self, url, params=None):
        """Makes a request to the ListenBrainz API."""
        try:
            response = requests.get(
                url=url,
                headers=self.AUTH_HEADER,
                timeout=10,
                params=params,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self._log.debug(f"Invalid Search Error: {e}")
            return None

    def get_listens(self, min_ts=None, max_ts=None, count=None):
        """Gets the listen history of a given user.

        Args:
            username: User to get listen history of.
            min_ts: History before this timestamp will not be returned.
                    DO NOT USE WITH max_ts.
            max_ts: History after this timestamp will not be returned.
                    DO NOT USE WITH min_ts.
            count: How many listens to return. If not specified,
                uses a default from the server.

        Returns:
            A list of listen info dictionaries if there's an OK status.

        Raises:
            An HTTPError if there's a failure.
            A ValueError if the JSON in the response is invalid.
            An IndexError if the JSON is not structured as expected.
        """
        url = f"{self.ROOT}/user/{self.username}/listens"
        params = {
            k: v
            for k, v in {
                "min_ts": min_ts,
                "max_ts": max_ts,
                "count": count,
            }.items()
            if v is not None
        }
        response = self._make_request(url, params)

        if response is not None:
            return response["payload"]["listens"]
        else:
            return None

    def get_tracks_from_listens(self, listens):
        """Returns a list of tracks from a list of listens."""
        tracks = []
        for track in listens:
            if track["track_metadata"].get("release_name") is None:
                continue
            mbid_mapping = track["track_metadata"].get("mbid_mapping", {})
            # print(json.dumps(track, indent=4, sort_keys=True))
            if mbid_mapping.get("recording_mbid") is None:
                # search for the track using title and release
                mbid = self.get_mb_recording_id(track)
            tracks.append(
                {
                    "album": {
                        "name": track["track_metadata"].get("release_name")
                    },
                    "name": track["track_metadata"].get("track_name"),
                    "artist": {
                        "name": track["track_metadata"].get("artist_name")
                    },
                    "mbid": mbid,
                    "release_mbid": mbid_mapping.get("release_mbid"),
                    "listened_at": track.get("listened_at"),
                }
            )
        return tracks

    def get_mb_recording_id(self, track):
        """Returns the MusicBrainz recording ID for a track."""
        resp = musicbrainzngs.search_recordings(
            query=track["track_metadata"].get("track_name"),
            release=track["track_metadata"].get("release_name"),
            strict=True,
        )
        if resp.get("recording-count") == "1":
            return resp.get("recording-list")[0].get("id")
        else:
            return None

    def get_playlists_createdfor(self, username):
        """Returns a list of playlists created by a user."""
        url = f"{self.ROOT}/user/{username}/playlists/createdfor"
        return self._make_request(url)

    def get_listenbrainz_playlists(self):
        """Returns a list of playlists created by ListenBrainz."""
        import re

        resp = self.get_playlists_createdfor(self.username)
        playlists = resp.get("playlists")
        listenbrainz_playlists = []

        for playlist in playlists:
            playlist_info = playlist.get("playlist")
            if playlist_info.get("creator") == "listenbrainz":
                title = playlist_info.get("title")
                match = re.search(
                    r"(Missed Recordings of \d{4}|Discoveries of \d{4})", title
                )
                if "Exploration" in title:
                    playlist_type = "Exploration"
                elif "Jams" in title:
                    playlist_type = "Jams"
                elif match:
                    playlist_type = match.group(1)
                else:
                    playlist_type = None
                if "week of " in title:
                    date_str = title.split("week of ")[1].split(" ")[0]
                    date = datetime.datetime.strptime(
                        date_str, "%Y-%m-%d"
                    ).date()
                else:
                    date = None
                identifier = playlist_info.get("identifier")
                id = identifier.split("/")[-1]
                if playlist_type in ["Jams", "Exploration"]:
                    listenbrainz_playlists.append(
                        {
                            "type": playlist_type,
                            "date": date,
                            "identifier": id,
                            "title": title,
                        }
                    )
        return listenbrainz_playlists

    def get_playlist(self, identifier):
        """Returns a playlist."""
        url = f"{self.ROOT}/playlist/{identifier}"
        return self._make_request(url)

    def get_tracks_from_playlist(self, playlist):
        """This function returns a list of tracks in the playlist."""
        tracks = []
        for track in playlist.get("playlist").get("track"):
            tracks.append(
                {
                    "artist": track.get("creator"),
                    "identifier": track.get("identifier").split("/")[-1],
                    "title": track.get("title"),
                }
            )
        return self.get_track_info(tracks)

    def get_track_info(self, tracks):
        """Returns a list of track info."""
        track_info = []
        for track in tracks:
            identifier = track.get("identifier")
            resp = musicbrainzngs.get_recording_by_id(
                identifier, includes=["releases", "artist-credits"]
            )
            recording = resp.get("recording")
            title = recording.get("title")
            artist_credit = recording.get("artist-credit", [])
            if artist_credit:
                artist = artist_credit[0].get("artist", {}).get("name")
            else:
                artist = None
            releases = recording.get("release-list", [])
            if releases:
                album = releases[0].get("title")
                date = releases[0].get("date")
                year = date.split("-")[0] if date else None
            else:
                album = None
                year = None
            track_info.append(
                {
                    "identifier": identifier,
                    "title": title,
                    "artist": artist,
                    "album": album,
                    "year": year,
                }
            )
        return track_info

    def get_weekly_playlist(self, index):
        """Returns a list of weekly playlists based on the index."""
        playlists = self.get_listenbrainz_playlists()
        playlist = self.get_playlist(playlists[index].get("identifier"))
        self._log.info(f"Getting {playlist.get('playlist').get('title')}")
        return self.get_tracks_from_playlist(playlist)

    def get_weekly_exploration(self):
        """Returns a list of weekly exploration."""
        return self.get_weekly_playlist(0)

    def get_weekly_jams(self):
        """Returns a list of weekly jams."""
        return self.get_weekly_playlist(1)

    def get_last_weekly_exploration(self):
        """Returns a list of weekly exploration."""
        return self.get_weekly_playlist(3)

    def get_last_weekly_jams(self):
        """Returns a list of weekly jams."""
        return self.get_weekly_playlist(3)

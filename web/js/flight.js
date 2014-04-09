var ws;
var map;
var openedInfoWindow;
var flightsHash = {};
var airportName = {};
var markers = [];
var polylines = [];
var polygons = [];

function appendMsg(ws_logs, msg) {
  var scroll = ws_logs.parent()[0];
  var at_bottom = (scroll.offsetHeight + scroll.scrollTop >= scroll.scrollHeight);
  ws_logs.append('<li>' + msg + '</li>');
  if (at_bottom) {
    scroll.scrollTop = scroll.scrollHeight;
  }
}

function newFlight(d_lat, d_long, d_time, d_id, a_lat, a_long, a_time, a_id) {
  return {
    d_lat: d_lat,
    d_long: d_long,
    d_time: d_time,
    d_id: d_id,
    a_lat: a_lat,
    a_long: a_long,
    a_time: a_time,
    a_id: a_id
  }
}

function validateFlight(f) {
  return (
    f.d_lat && f.d_long && f.d_time && f.d_id &&
    f.a_lat && f.a_long && f.a_time && f.a_id && 
    !isNaN(f.d_lat) && !isNaN(f.d_long) && !isNaN(f.d_time) && !isNaN(f.d_id) && 
    !isNaN(f.a_lat) && !isNaN(f.a_long) && !isNaN(f.a_time) && !isNaN(f.a_id)
  )
}

function encodeFlight(f) {
  f.d_lat = Math.round((parseFloat(f.d_lat)+360)*1000);
  f.d_long = Math.round((parseFloat(f.d_long)+360)*1000);
  f.a_lat = Math.round((parseFloat(f.a_lat)+360)*1000);
  f.a_long = Math.round((parseFloat(f.a_long)+360)*1000);
}

function bindOps(ws_logs) {
  $('#insert').unbind();
  $('#find').unbind();
  $('#delete').unbind();
  $('#range').unbind();
  $('#origin_sw').unbind();
  $('#origin_ne').unbind();
  $('#dest_sw').unbind();
  $('#dest_ne').unbind();

  $('#insert').click(function() {
      var f = newFlight(
        $('#insert_lat_origin').val(),
        $('#insert_long_origin').val(),
        $('#insert_dep_time').val(),
        $('#insert_dep_airport').val(),
        $('#insert_lat_dest').val(),
        $('#insert_long_dest').val(),
        $('#insert_arr_time').val(),
        $('#insert_arr_airport').val()
        );
      var w = $('#insert_w').val();

      if (
        ws.readyState == WebSocket.OPEN && validateFlight(f) && w && !isNaN(w))
      {
        encodeFlight(f);
        var msg = ["INSERT", f.d_lat, f.d_long, f.d_time,
              f.a_lat, f.a_long, f.a_time, f.d_id, f.a_id, w].join();
        ws.send(msg);
      } else if (ws.readyState == WebSocket.OPEN) {
        appendMsg(ws_logs, "Missing/invalid argument(s)");
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
  });

  $('#delete').click(function() {
      var f = newFlight(
        $('#delete_lat_origin').val(),
        $('#delete_long_origin').val(),
        $('#delete_dep_time').val(),
        $('#delete_dep_airport').val(),
        $('#delete_lat_dest').val(),
        $('#delete_long_dest').val(),
        $('#delete_arr_time').val(),
        $('#delete_arr_airport').val()
        );
      if (ws.readyState == WebSocket.OPEN && validateFlight(f)) {
        encodeFlight(f);
        var msg = ["DELETE", f.d_lat, f.d_long, f.d_time,
        f.a_lat, f.a_long, f.a_time, f.d_id, f.a_id].join();
        ws.send(msg);
      } else if (ws.readyState == WebSocket.OPEN) {
        appendMsg(ws_logs, "Missing/invalid argument(s)");
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
  });

  $('#find').click(function() {
      var f = newFlight(
        $('#find_lat_origin').val(),
        $('#find_long_origin').val(),
        $('#find_dep_time').val(),
        $('#find_dep_airport').val(),
        $('#find_lat_dest').val(),
        $('#find_long_dest').val(),
        $('#find_arr_time').val(),
        $('#find_arr_airport').val()
        );
      if (ws.readyState == WebSocket.OPEN && validateFlight(f)) {
        encodeFlight(f);
        var msg = ["FIND", f.d_lat, f.d_long, f.d_time,
              f.a_lat, f.a_long, f.a_time, f.d_id, f.a_id].join();
        ws.send(msg);
      } else if (ws.readyState == WebSocket.OPEN) {
        appendMsg(ws_logs, "Missing/invalid argument(s)");
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
  });

  $('#range').click(function() {
      var MAX_C_LONG_LONG = '9223372036854775807';
      var f1 = newFlight(
        $('#range_lat_origin_1').val(),
        $('#range_long_origin_1').val(),
        '0', //$('#range_dep_time_1').val(),
        '0', //$('#range_dep_airport_1').val(),
        $('#range_lat_dest_1').val(),
        $('#range_long_dest_1').val(),
        '0', //$('#range_arr_time_1').val(),
        '0' //$('#range_arr_airport_1').val()
        );
      var f2 = newFlight(
        $('#range_lat_origin_2').val(),
        $('#range_long_origin_2').val(),
        MAX_C_LONG_LONG, //$('#range_dep_time_2').val(),
        MAX_C_LONG_LONG, //$('#range_dep_airport_2').val(),
        $('#range_lat_dest_2').val(),
        $('#range_long_dest_2').val(),
        MAX_C_LONG_LONG, //$('#range_arr_time_2').val(),
        MAX_C_LONG_LONG //$('#range_arr_airport_2').val()
        );

      if (
          ws.readyState == WebSocket.OPEN && validateFlight(f1) && validateFlight(f2)
         ) {
        encodeFlight(f1);
        encodeFlight(f2);
        var msg = ["RANGE",
                    f1.d_lat, f1.d_long, f1.d_time, f1.a_lat,
                    f1.a_long, f1.a_time, f1.d_id, f1.a_id,
                    f2.d_lat, f2.d_long, f2.d_time, f2.a_lat,
                    f2.a_long, f2.a_time, f2.d_id, f2.a_id].join();
        ws.send(msg);

      } else if (ws.readyState == WebSocket.OPEN) {
        appendMsg(ws_logs, "Missing/invalide argument(s)");
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
  });

  $('#range_origin_1').click(pickerHandle);
  $('#range_origin_2').click(pickerHandle);
  $('#range_dest_1').click(pickerHandle);
  $('#range_dest_2').click(pickerHandle);

  $('#close').click(function() {
      ws.close();
      $('#if').hide();
      $('#start_ws').show();
      });
}

function pickerHandle() {
  resetPickers();
  var id = $(this).attr('id');
  $(this).text('Click map now');
  $(this).attr('disabled', 'disabled');
  google.maps.event.clearListeners(map, 'click');
  google.maps.event.addListener(map, "click", function(e) {
    var latLng = e.latLng;
    switch (id) {
      case 'range_origin_1':
        $('#range_lat_origin_1').val(latLng.lat());
        $('#range_long_origin_1').val(latLng.lng());
        break;
      case 'range_origin_2':
        $('#range_lat_origin_2').val(latLng.lat());
        $('#range_long_origin_2').val(latLng.lng());
        break;
      case 'range_dest_1':
        $('#range_lat_dest_1').val(latLng.lat());
        $('#range_long_dest_1').val(latLng.lng());
        break;
      case 'range_dest_2':
        $('#range_lat_dest_2').val(latLng.lat());
        $('#range_long_dest_2').val(latLng.lng());
        break;
      default:
        break;
    }
    resetPickers();
  });
}

function resetPickers() {
  $('button.picker').text('Select on map');
  $('button.picker').removeAttr('disabled');
  google.maps.event.clearListeners(map, 'click');
  google.maps.event.addListener(map, "click", defaultMapClickHandle);
}

function sendDeltaTimeOp(list, ws_logs) {
  if (list.length) {
    var entry = list.pop();
    window.setTimeout(function() {
      if (ws.readyState == WebSocket.OPEN) {
        ws.send(entry.op);
        sendDeltaTimeOp(list);
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
    }, entry.delta);
  }
}

function bindSendMessage(ws_logs) {
  $('#send').unbind();

  $('#send').click(function() {
    var message = $('#message').val();
    if (message && ws.readyState == WebSocket.OPEN) {
      ws.send(message);
    } else if (ws.readyState == WebSocket.OPEN) {
      appendMsg(ws_logs, "Missing argument(s)");
    } else {
      appendMsg(ws_logs, "WebSocket connection error");
    }
  });
}

function bindLoadNameMap(ws_logs) {
  $('#name_map').unbind();

  $('#name_map').change(function(evt) {
    var f = evt.target.files[0];
    var reader = new FileReader();
    reader.onload = function(prog) {
      var content = prog.target.result;
      var entries = content.split("\n");
      entries.pop();
      if (ws.readyState == WebSocket.OPEN) {
        $.each(entries, function(idx, entry) {
          var values = entry.split(",")
          ws.send("NAME_INSERT,"+values[0]+","+values[1]);
        });
      } else {
        appendMsg(ws_logs, "WebSocket connection error");
      }
    };
    reader.readAsText(f);
    evt.preventDefault();
  });
}

function bindLoadFile(ws_logs) {
  $('#load_file').unbind();

  $('#load_file').change(function(evt) {
    var f = evt.target.files[0];
    var reader = new FileReader();
    reader.onload = function(prog) {
      var content = prog.target.result;
      var ops = content.split("\n");
      var i;
      var delta_list = [];
      for (i = 0; i < ops.length; i++) {
        var targs = ops[i].trim().split(":",2);
        if (targs.length != 2) {
          console.log("Missing delta time on line " + i + ": " + ops[i]);
          continue;
        }
        var delta = targs[0];
        if (isNaN(parseFloat(delta)) || !isFinite(delta)) {
          console.log("Invalid delta time on line " + i + ": " + ops[i]);
          continue;
        }
        var args = targs[1].trim().split(",");
        if (args.length) {
          var opstr = args[0].toUpperCase();
          var opcode = $.inArray(opstr, ["INSERT", "DELETE", "FIND", "RANGE"])
          var num_args = [4,3,3,5];
          if (opcode > -1 && args.length == num_args[opcode]) {
            delta_list.push({"op":targs[1], "delta":delta})
            continue;
          }
        }
        console.log("Invalid operation on line " + i + ": " + ops[i]);
      }
      console.log(delta_list);
      sendDeltaTimeOp(delta_list.reverse(), ws_logs);
    };
    reader.readAsText(f);
    evt.preventDefault();
  });
}

function decodeResult(result) {
  var t, decoded;
  decoded = {};
  decoded.lat_dest = (result.lat_dest/1000 - 360).toFixed(3);
  decoded.long_dest = (result.long_dest/1000 - 360).toFixed(3);
  decoded.lat_origin = (result.lat_origin/1000 - 360).toFixed(3);
  decoded.long_origin = (result.long_origin/1000 - 360).toFixed(3);
  decoded.airport_dest = result.airport_dest;
  decoded.airport_origin = result.airport_origin;
  t = new Date(result.dep_time*1000);
  decoded.dep_time = t.toGMTString();
  t = new Date(result.arr_time*1000);
  decoded.arr_time = t.toGMTString();
  decoded.success = result.success;
  decoded.duplicate = result.duplicate;
  decoded.pid = result.pid;
  return decoded;
}

function decodeRangeResult(result) {
  var t, decoded;
  decoded = {};
  t  = new Date(result.dep_time_1*1000);
  decoded.dep_time_1_str = t.toGMTString();
  t = new Date(result.arr_time_1*1000);
  decoded.arr_time_1_str = t.toGMTString();
  decoded.lat_dest_1 = (result.lat_dest_1/1000 - 360).toFixed(3);
  decoded.long_dest_1 = (result.long_dest_1/1000 - 360).toFixed(3);
  decoded.lat_origin_1 = (result.lat_origin_1/1000 - 360).toFixed(3);
  decoded.long_origin_1 = (result.long_origin_1/1000 - 360).toFixed(3);
  t  = new Date(result.dep_time_2*1000);
  decoded.dep_time_2_str = t.toGMTString();
  t = new Date(result.arr_time_2*1000);
  decoded.arr_time_2_str = t.toGMTString();
  decoded.lat_dest_2 = (result.lat_dest_2/1000 - 360).toFixed(3);
  decoded.long_dest_2 = (result.long_dest_2/1000 - 360).toFixed(3);
  decoded.lat_origin_2 = (result.lat_origin_2/1000 - 360).toFixed(3);
  decoded.long_origin_2 = (result.long_origin_2/1000 - 360).toFixed(3);
  decoded.airport_dest_1 = result.airport_dest_1;
  decoded.airport_dest_2 = result.airport_dest_2;
  decoded.airport_origin_1 = result.airport_origin_1;
  decoded.airport_origin_2 = result.airport_origin_2;
  decoded.entries = [];
  $.each(result.entries, function (idx, entry) {
    decoded.entries.push(decodeResult(entry));
  });
  decoded.sum = result.sum;
  decoded.num_entries = result.num_entries;
  decoded.pid = result.pid;
  return decoded;
}

function startWS()
{
  $('#start_ws').hide();

  var ws_logs = $('#ws_logs');
  ws_logs.empty();

  if ("WebSocket" in window) {
    ws = new WebSocket("ws://" + window.location.host + "/sub");

    appendMsg(ws_logs, "Opening WebSocket Connection...");

    ws.onopen = function () {
      appendMsg(ws_logs, "Opened WebSocket Connection.");

      // Show application interface
      $('#if').show();
      bindOps(ws_logs);
      bindSendMessage(ws_logs);
      bindLoadNameMap(ws_logs);
      initMap();
    };

    ws.onmessage = function(event) {
      var msg = event.data;
      try {
        var result = JSON.parse(msg);
        var decoded;

        if ("op" in result && $.inArray(result.op, ["INSERT","DELETE","FIND","RANGE"]) > -1) {
          if (result.op == "RANGE") {
            decoded = decodeRangeResult(result);
            clearMap();
            $.each(decoded.entries, function(idx, entry) {
              insertHandler(entry);
            });

            var origin_bound = [
              new google.maps.LatLng(decoded.lat_origin_1, decoded.long_origin_1),
              new google.maps.LatLng(decoded.lat_origin_1, decoded.long_origin_2),
              new google.maps.LatLng(decoded.lat_origin_2, decoded.long_origin_2),
              new google.maps.LatLng(decoded.lat_origin_2, decoded.long_origin_1),
            ];
            var dest_bound = [
              new google.maps.LatLng(decoded.lat_dest_1, decoded.long_dest_1),
              new google.maps.LatLng(decoded.lat_dest_1, decoded.long_dest_2),
              new google.maps.LatLng(decoded.lat_dest_2, decoded.long_dest_2),
              new google.maps.LatLng(decoded.lat_dest_2, decoded.long_dest_1),
            ];
            var origin_polygon = new google.maps.Polygon({
              path: origin_bound,
              strokeColor: '#00FF00',
              strokeOpacity: 0.5,
              strokeWeight: 1,
              fillColor: '#00FF00',
              fillOpacity: 0.1,
              clickable: false,
              geodesic: true
            });
            var dest_polygon = new google.maps.Polygon({
              path: dest_bound,
              strokeColor: '#FF0000',
              strokeOpacity: 0.5,
              strokeWeight: 1,
              fillColor: '#FF0000',
              fillOpacity: 0.1,
              clickable: false,
              geodesic: true
            });
            origin_polygon.setMap(map);
            dest_polygon.setMap(map);
            polygons.push(origin_polygon);
            polygons.push(dest_polygon);
            
            appendMsg(ws_logs, JSON.stringify(decoded));

          } else if (result.success == 1 &&
              (result.op == "INSERT" || result.op == "FIND")) {
            decoded = decodeResult(result);
            insertHandler(decoded);
            appendMsg(ws_logs, JSON.stringify(decoded));

          } else if (result.op == "DELETE" && result.success == 1) {
            decoded = decodeResult(result);
            deleteHandler(decoded);
            appendMsg(ws_logs, JSON.stringify(decoded));

          } else {
            decoded = decodeResult(result);
            appendMsg(ws_logs, JSON.stringify(decoded));

          }
        } else {
          if (result.op == "NAME_LOOKUP" && result.success == 1) {
            airportName[result.key] = result.value;
            if (openedInfoWindow && ('anchor' in openedInfoWindow)) {
              createInfoWindow(openedInfoWindow.anchor);
            }
          }

          appendMsg(ws_logs, JSON.stringify(result));
        }


      } catch (e) {
        console.log(e);
        appendMsg(ws_logs, msg);
      }
    };

    ws.onclose = function() {
      appendMsg(ws_logs, "Closed WebSocket Connection");
      $('#if').hide();
      $('#start_ws').show();
    };

  } else {
    appendMsg(ws_logs, "Your browser does not support WebSocket");
  }
}

function insertHandler(result) {

  var origin = createInfoMarker(result.lat_origin, result.long_origin,
      result.dep_time, result.airport_origin);

  var dest = createInfoMarker(result.lat_dest, result.long_dest,
      result.arr_time, result.airport_dest);

  var arrowSymbol = {
    path: google.maps.SymbolPath.FORWARD_CLOSED_ARROW,
    scale: 1.5
  };

  var polyOptions= {
    strokeColor: '#0000FF',
    strokeOpacity: 0.3,
    strokeWeight: 1,
    geodesic: true,
    map: map,
    icons: [{
      icon: arrowSymbol,
      repeat: '50px'
    }]
  };

  var poly = new google.maps.Polyline(polyOptions);

  var path = [origin.getPosition(), dest.getPosition()];

  poly.setPath(path);

  var flightKeys = ["lat_origin", "long_origin", "dep_time", "lat_dest",
      "long_dest", "arr_time", "airport_origin", "airport_dest"];

  var flight = subhash(result, flightKeys);
  var key = JSON.stringify(flight);

  if (!(key in flightsHash)) {
    flightsHash[JSON.stringify(flight)] = {
      origin_marker: origin,
      dest_marker: dest,
      polyline: poly
    };
  }
  markers.push(origin);
  markers.push(dest);
  polylines.push(poly); 
}

function deleteHandler(result) {
  var flightKeys = ["lat_origin", "long_origin", "dep_time", "lat_dest",
      "long_dest", "arr_time", "airport_origin", "airport_dest"];
  var flight = subhash(result, flightKeys);
  var key = JSON.stringify(flight);
  if (key in flightsHash) {
    var value = flightsHash[key];
    value.origin_marker.setMap(null);
    value.dest_marker.setMap(null);
    value.polyline.setMap(null);
    delete flightsHash[key];
  }
}

function clearMap() {
  var temp = ws.onmessage;
  ws.onmessage = null;
  $.each(markers, function(idx, marker) {
    marker.setMap(null);
  });
  $.each(polylines, function(idx, line) {
    line.setMap(null);
  });
  $.each(polygons, function(idx, gon) {
    gon.setMap(null);
  });
  flightsHash = {};
  ws.onmessage = temp;
}

function subhash(source, keys) {
  var newObject = {};
  keys.forEach(function(key) {
    newObject[key] = source[key];
  });
  return newObject;
}

function createInfoWindow(marker) {
  if (!(marker.airportID in airportName)) {
    ws.send("NAME_LOOKUP," + marker.airportID);
  }

  var contentString = '<ul class="marker_info">'+
  '<li>LatLng: '+marker.getPosition().toUrlValue(3)+'</li>'+
  '<li>Airport ID: '+marker.airportID+'</li>'+
  ((marker.airportID in airportName)?
   ('<li>Airport Name: '+airportName[marker.airportID]+'</li>'):'')+
  '</ul>';

  var infoWindow = new google.maps.InfoWindow({
    content: contentString
  });
  if (openedInfoWindow) {
    openedInfoWindow.close();
  }
  infoWindow.open(map, marker);
  openedInfoWindow = infoWindow;
}

function createInfoMarker(lat, lng, timeStr, airportID) {
  var marker = new google.maps.Marker({
    map: map,
    position: new google.maps.LatLng(lat, lng),
    gmtString: timeStr,
    airportID: airportID
  });

  google.maps.event.addListener(marker, "click", function() {
    createInfoWindow(marker);
  });

  return marker;
}

function defaultMapClickHandle(e) {
  var latLng = e.latLng;
  var infoWindow = new google.maps.InfoWindow({
    content: latLng.toUrlValue(3),
    position: latLng
  });
  if (openedInfoWindow) {
    openedInfoWindow.close();
  }
  infoWindow.open(map);
  openedInfoWindow = infoWindow;
}

function initMap() {
  var mapOptions = {
    zoom: 4,
    center: new google.maps.LatLng(39.50, -98.35)
  }
  map = new google.maps.Map(document.getElementById('map-canvas'), mapOptions);
  google.maps.event.addListener(map, "click", defaultMapClickHandle);
}
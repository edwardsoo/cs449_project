//
//  echolalia.js
//
//  Created by Christian Swinehart on 2010-12-15.
//  Copyright (c) 2011 Samizdat Drafting Co. All rights reserved.
//

(function($){

  DeadSimpleRenderer = function(canvas){
    var canvas = $(canvas).get(0);
    var ctx = canvas.getContext("2d");
    var particleSystem = null;
    var selected = null;
    var nearest = null;
    var _mouseP = null;
    var printed = false;

    var that = {
      //
      // the particle system will call the init function once, right before the
      // first frame is to be drawn. it's a good place to set up the canvas and
      // to pass the canvas size to the particle system
      //
      init:function(system){
        // save a reference to the particle system for use in the .redraw() loop
        particleSystem = system;

        // inform the system of the screen dimensions so it can map coords for us.
        // if the canvas is ever resized, screenSize should be called again with
        // the new dimensions
        particleSystem.screenSize(canvas.width, canvas.height);
        // leave an extra 80px of whitespace per side
        particleSystem.screenPadding(20); 
      },
      
      // 
      // redraw will be called repeatedly during the run whenever the node positions
      // change. the new positions for the nodes can be accessed by looking at the
      // .p attribute of a given node. however the p.x & p.y values are in the coordinates
      // of the particle system rather than the screen. you can either map them to
      // the screen yourself, or use the convenience iterators .eachNode (and .eachEdge)
      // which allow you to step through the actual node objects but also pass an
      // x,y point in the screen's coordinate system
      // 

      redraw:function(){
        ctx.clearRect(0,0, canvas.width, canvas.height);
        
        particleSystem.eachEdge(function(edge, pt1, pt2){
          // edge: {source:Node, target:Node, length:#, data:{}}
          // pt1:  {x:#, y:#}  source position in screen coords
          // pt2:  {x:#, y:#}  target position in screen coords

          var tri_base = 3;
          var tri_height = 5;
          var margin = 5;
          var vec = pt2.subtract(pt1);
          var dist = vec.magnitude();
          var oth_vec = vec.normal();
          var tmp_pt;
          vec = vec.normalize();
          oth_vec = oth_vec.normalize();

          // draw a line from pt1 to pt2
          ctx.strokeStyle = "rgba(0,0,0,0.5)";
          ctx.fillStyle = "rgba(0,0,0,0.5)";
          ctx.lineWidth = 1;
          ctx.beginPath();

          ctx.moveTo(pt1.x, pt1.y);
          tmp_pt = pt2.subtract(vec.multiply(tri_height + margin));
          ctx.lineTo(tmp_pt.x, tmp_pt.y);
          ctx.stroke();
          ctx.closePath();

          // Draw an arrow
          ctx.beginPath();
          tmp_pt = pt2.subtract(vec.multiply(margin));
          ctx.moveTo(tmp_pt.x, tmp_pt.y);
          tmp_pt = tmp_pt.subtract(vec.multiply(tri_height));
          tmp_pt = tmp_pt.add(oth_vec.multiply(tri_base/2));
          ctx.lineTo(tmp_pt.x, tmp_pt.y);
          tmp_pt = tmp_pt.subtract(oth_vec.multiply(tri_base));
          ctx.lineTo(tmp_pt.x, tmp_pt.y);
          tmp_pt = pt2.subtract(vec.multiply(margin));
          ctx.lineTo(tmp_pt.x, tmp_pt.y);
          ctx.fill();
          ctx.stroke();
          ctx.closePath();

          // Label edge with weight
          var show_weight = $('#show_weight');
          if (show_weight.is(':checked')) {
            ctx.fillStyle = "rgba(0,0,0,1)";
            tmp_pt = pt2.subtract(vec.multiply(dist * 2/3));
            ctx.fillText(edge.data.weight, tmp_pt.x, tmp_pt.y);
          }
          
        })

        particleSystem.eachNode(function(node, pt){
          // node: {mass:#, p:{x,y}, name:"", data:{}}
          // pt:   {x:#, y:#}  node position in screen coords

          // draw a circle centered at pt
          var r = 5;
          ctx.fillStyle = "rgba(100,100,255,1)";
          ctx.beginPath();
          ctx.arc(pt.x, pt.y, r, 0, 2*Math.PI);
          ctx.fill();

          // Label vertex
          var use_name = $('#use_name');
          ctx.font="14px Verdana";
          ctx.fillStyle = "rgba(0,0,0,1)";
          if (use_name.is(':checked') && 'alt' in node){
            ctx.fillText(node.alt, pt.x, pt.y + 4*r);
          } else {
            ctx.fillText(node.name, pt.x, pt.y + 4*r);
          }
        })    			
      }
    }
    return that;
  }    

  $(document).ready(function(){
    sys = arbor.ParticleSystem(1000, 100, 0.5); // create the system with sensible repulsion/stiffness/friction
    sys.renderer = DeadSimpleRenderer("#viewport"); // our newly created renderer will have its .init() method called shortly by sys...

    canvas = $('#viewport')[0];
    window.addEventListener('resize', resizeCanvas, false);
    resizeCanvas();
  })

})(this.jQuery)
